// Copyright 2026 Northern.tech AS
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

#include <common/varlink/server.hpp>

#include <cassert>
#include <filesystem>
#include <istream>
#include <string>
#include <system_error>

#include <nlohmann/json.hpp>

#include <common/json.hpp>

namespace mender {
namespace common {
namespace varlink {

namespace fs = std::filesystem;

// Standard varlink service error names.
static const char *kErrorMethodNotFound = "org.varlink.service.MethodNotFound";
static const char *kErrorInvalidParameter = "org.varlink.service.InvalidParameter";

const ErrorCategoryClass ErrorCategory;

const char *ErrorCategoryClass::name() const noexcept {
	return "VarlinkErrorCategory";
}

string ErrorCategoryClass::message(int code) const {
	switch (code) {
	case NoError:
		return "Success";
	case ProtocolError:
		return "Varlink protocol error";
	case TransportError:
		return "Varlink transport error";
	}
	assert(false);
	return "Unknown";
}

error::Error MakeError(VarlinkErrorCode code, const string &msg) {
	return error::Error(error_condition(code, ErrorCategory), msg);
}

// Serializes a Reply into a NUL-terminated varlink frame.
static string SerializeFrame(const Reply &reply) {
	nlohmann::json frame;
	if (!reply.error_name.empty()) {
		frame["error"] = reply.error_name;
	}
	// parameters_json is expected to be a valid JSON object string; default "{}".
	frame["parameters"] = nlohmann::json::parse(reply.parameters_json, nullptr, false);
	if (frame["parameters"].is_discarded()) {
		frame["parameters"] = nlohmann::json::object();
	}
	if (reply.continues) {
		frame["continues"] = true;
	}
	string out = frame.dump();
	out.push_back(kMessageSeparator);
	return out;
}

//
// Connection
//

Connection::Connection(Server &server, asio::io_context &ctx) :
	server_ {server},
	socket_ {ctx},
	cancelled_ {std::make_shared<bool>(false)},
	logger_ {"varlink"} {
}

void Connection::Start() {
	ReadFrame();
}

void Connection::Close() {
	*cancelled_ = true;
	boost::system::error_code ec;
	socket_.close(ec);
}

void Connection::ReadFrame() {
	auto cancelled = cancelled_;
	auto self = shared_from_this();
	asio::async_read_until(
		socket_,
		buf_,
		kMessageSeparator,
		[self, cancelled](const boost::system::error_code &ec, size_t num_read) {
			if (*cancelled) {
				return;
			}
			self->ReadFrameHandler(ec, num_read);
		});
}

void Connection::ReadFrameHandler(const boost::system::error_code &ec, size_t num_read) {
	if (ec) {
		if (ec != asio::error::eof && ec != asio::error::operation_aborted) {
			logger_.Debug("Error reading varlink frame: " + ec.message());
		}
		server_.RemoveConnection(shared_from_this());
		return;
	}

	// async_read_until may read past the separator; only consume up to and including it.
	std::istream is(&buf_);
	string message;
	std::getline(is, message, kMessageSeparator);

	Dispatch(message);

	if (*cancelled_) {
		return;
	}

	// Continue reading the next frame on the same connection.
	ReadFrame();
}

bool Connection::WriteReply(const Reply &reply) {
	if (*cancelled_) {
		return false;
	}

	// Queue the frame and ensure exactly one async_write is in flight at a time.
	// Concurrent async_write on the same socket is undefined behaviour in Asio,
	// and the streaming Monitor feed can produce frames faster than they drain
	// (e.g. a download-progress signal per percent).
	write_queue_.push_back(std::make_shared<string>(SerializeFrame(reply)));
	if (!writing_) {
		DoWrite();
	}
	return true;
}

void Connection::DoWrite() {
	if (write_queue_.empty()) {
		writing_ = false;
		return;
	}
	writing_ = true;
	auto frame = write_queue_.front();
	auto cancelled = cancelled_;
	auto self = shared_from_this();
	asio::async_write(
		socket_,
		asio::buffer(*frame),
		[self, cancelled, frame](const boost::system::error_code &ec, size_t) {
			if (*cancelled) {
				return;
			}
			if (ec) {
				self->logger_.Debug("Error writing varlink reply: " + ec.message());
				self->server_.RemoveConnection(self);
				return;
			}
			self->write_queue_.pop_front();
			self->DoWrite();
		});
}

void Connection::Dispatch(const string &message) {
	auto exp_json = json::Load(message);
	if (!exp_json) {
		Reply err;
		err.error_name = kErrorInvalidParameter;
		err.parameters_json = R"({"parameter":"<message>"})";
		WriteReply(err);
		return;
	}
	const json::Json &j = exp_json.value();

	auto exp_method = j.Get("method");
	if (!exp_method) {
		Reply err;
		err.error_name = kErrorInvalidParameter;
		err.parameters_json = R"({"parameter":"method"})";
		WriteReply(err);
		return;
	}
	auto exp_method_str = exp_method.value().GetString();
	if (!exp_method_str) {
		Reply err;
		err.error_name = kErrorInvalidParameter;
		err.parameters_json = R"({"parameter":"method"})";
		WriteReply(err);
		return;
	}

	MethodCall call;
	call.method = exp_method_str.value();

	auto exp_params = j.Get("parameters");
	if (exp_params) {
		call.parameters = exp_params.value();
	}

	auto exp_more = j.Get("more");
	if (exp_more) {
		auto b = exp_more.value().GetBool();
		if (b) {
			call.more = b.value();
		}
	}

	auto exp_oneway = j.Get("oneway");
	if (exp_oneway) {
		auto b = exp_oneway.value().GetBool();
		if (b) {
			call.oneway = b.value();
		}
	}

	const MethodHandler *handler = server_.LookupMethod(call.method);
	if (handler == nullptr) {
		Reply err;
		err.error_name = kErrorMethodNotFound;
		err.parameters_json =
			nlohmann::json {{"method", call.method}}.dump();
		WriteReply(err);
		return;
	}

	// The Replier emits a non-final (streaming) frame. It forces continues=true so
	// callers don't have to set it for intermediate replies. It captures a WEAK
	// reference so a long-lived subscriber (e.g. a Monitor observer) does not keep
	// the connection alive after the client disconnects -- it just returns false
	// once the connection is gone, so the subscriber can drop itself.
	std::weak_ptr<Connection> weak = shared_from_this();
	Replier emit = [weak](const Reply &reply) -> bool {
		auto conn = weak.lock();
		if (!conn) {
			return false;
		}
		Reply streamed = reply;
		streamed.continues = true;
		return conn->WriteReply(streamed);
	};

	Reply terminal = (*handler)(call, emit);

	if (*cancelled_) {
		return;
	}

	// Oneway calls expect no reply at all.
	if (call.oneway) {
		return;
	}

	WriteReply(terminal);
}

//
// Server
//

Server::Server(events::EventLoop &loop) :
	loop_ {loop},
	ctx_ {GetAsioIoContext(loop)},
	acceptor_ {ctx_},
	logger_ {"varlink_server"} {
}

Server::~Server() {
	Stop();
}

void Server::RegisterMethod(const string &method, MethodHandler handler) {
	methods_[method] = std::move(handler);
}

void Server::SetServiceInfo(
	const string &vendor, const string &product, const string &version, const string &url) {
	svc_vendor_ = vendor;
	svc_product_ = product;
	svc_version_ = version;
	svc_url_ = url;
}

void Server::AddInterface(const string &name, const string &description) {
	interface_descriptions_[name] = description;
}

void Server::RegisterServiceInterface() {
	// Standard introspection interface that every varlink service exposes, so
	// generic clients (varlinkctl, libvarlink, ...) can discover this service.
	RegisterMethod(
		"org.varlink.service.GetInfo",
		[this](const MethodCall &, const Replier &) {
			nlohmann::json p;
			p["vendor"] = svc_vendor_;
			p["product"] = svc_product_;
			p["version"] = svc_version_;
			p["url"] = svc_url_;
			nlohmann::json interfaces = nlohmann::json::array();
			interfaces.push_back("org.varlink.service");
			for (const auto &kv : interface_descriptions_) {
				interfaces.push_back(kv.first);
			}
			p["interfaces"] = interfaces;
			Reply r;
			r.parameters_json = p.dump();
			return r;
		});
	RegisterMethod(
		"org.varlink.service.GetInterfaceDescription",
		[this](const MethodCall &call, const Replier &) {
			Reply r;
			auto exp_iface = call.parameters.Get("interface").and_then([](const json::Json &j) {
				return j.GetString();
			});
			if (!exp_iface) {
				r.error_name = kErrorInvalidParameter;
				r.parameters_json = R"({"parameter":"interface"})";
				return r;
			}
			auto it = interface_descriptions_.find(exp_iface.value());
			if (it == interface_descriptions_.end()) {
				r.error_name = "org.varlink.service.InterfaceNotFound";
				r.parameters_json =
					nlohmann::json {{"interface", exp_iface.value()}}.dump();
				return r;
			}
			r.parameters_json = nlohmann::json {{"description", it->second}}.dump();
			return r;
		});
}

const MethodHandler *Server::LookupMethod(const string &method) const {
	auto it = methods_.find(method);
	if (it == methods_.end()) {
		return nullptr;
	}
	return &it->second;
}

error::Error Server::Listen(const string &socket_path) {
	socket_path_ = socket_path;

	// Expose the standard introspection interface now that the app has registered
	// its own methods and interface descriptions.
	RegisterServiceInterface();

	std::error_code std_ec;

	// Create the parent directory if missing (mode 0755).
	fs::path path {socket_path};
	fs::path parent = path.parent_path();
	if (!parent.empty() && !fs::exists(parent, std_ec)) {
		fs::create_directories(parent, std_ec);
		if (std_ec) {
			return MakeError(
				TransportError,
				"Could not create directory " + parent.string() + ": " + std_ec.message());
		}
		fs::permissions(
			parent,
			fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exec
				| fs::perms::others_read | fs::perms::others_exec,
			std_ec);
		if (std_ec) {
			logger_.Warning(
				"Could not set permissions on " + parent.string() + ": " + std_ec.message());
		}
	}

	// Unlink any stale socket file left over from a previous run.
	fs::remove(path, std_ec);

	boost::system::error_code ec;
	asio::local::stream_protocol::endpoint endpoint {socket_path};

	acceptor_.open(endpoint.protocol(), ec);
	if (ec) {
		return MakeError(TransportError, "Could not open varlink acceptor: " + ec.message());
	}

	acceptor_.bind(endpoint, ec);
	if (ec) {
		return MakeError(
			TransportError, "Could not bind varlink socket " + socket_path + ": " + ec.message());
	}

	acceptor_.listen(asio::socket_base::max_listen_connections, ec);
	if (ec) {
		return MakeError(TransportError, "Could not listen on varlink socket: " + ec.message());
	}

	// Restrict the socket to owner+group (0660). Without this the socket inherits
	// the process umask and may be world-connectable, letting any local user
	// control deployments (Continue/Abort) over the IPC.
	fs::permissions(
		path,
		fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read
			| fs::perms::group_write,
		fs::perm_options::replace,
		std_ec);
	if (std_ec) {
		logger_.Warning("Could not set permissions on varlink socket: " + std_ec.message());
	}

	AsyncAccept();

	return error::NoError;
}

void Server::AsyncAccept() {
	auto conn = std::make_shared<Connection>(*this, ctx_);
	acceptor_.async_accept(conn->Socket(), [this, conn](const boost::system::error_code &ec) {
		if (ec) {
			if (ec != asio::error::operation_aborted) {
				logger_.Error("Could not accept varlink connection: " + ec.message());
			}
			return;
		}

		connections_.insert(conn);
		conn->Start();

		AsyncAccept();
	});
}

void Server::RemoveConnection(const ConnectionPtr &conn) {
	conn->Close();
	connections_.erase(conn);
}

void Server::Stop() {
	boost::system::error_code ec;
	if (acceptor_.is_open()) {
		acceptor_.cancel(ec);
		acceptor_.close(ec);
	}

	for (auto &conn : connections_) {
		conn->Close();
	}
	connections_.clear();

	if (!socket_path_.empty()) {
		std::error_code std_ec;
		fs::remove(fs::path {socket_path_}, std_ec);
		socket_path_.clear();
	}
}

} // namespace varlink
} // namespace common
} // namespace mender
