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

#ifndef MENDER_COMMON_VARLINK_SERVER_HPP
#define MENDER_COMMON_VARLINK_SERVER_HPP

#include <deque>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include <boost/asio.hpp>

#include <common/error.hpp>
#include <common/events.hpp>
#include <common/log.hpp>
#include <common/varlink/varlink.hpp>

namespace mender {
namespace common {
namespace varlink {

namespace asio = boost::asio;
namespace events = mender::common::events;
namespace log = mender::common::log;

class Server;

// A single accepted varlink connection. Lives as long as the read/write loop is
// active. Held by the Server via shared_ptr in a set; removes itself on EOF/error.
class Connection : public std::enable_shared_from_this<Connection> {
public:
	Connection(Server &server, asio::io_context &ctx);

	asio::local::stream_protocol::socket &Socket() {
		return socket_;
	}

	void Start();
	void Close();

private:
	void ReadFrame();
	void ReadFrameHandler(const boost::system::error_code &ec, size_t num_read);
	void Dispatch(const std::string &message);
	// Serializes a Reply into a NUL-terminated frame and queues the write. Returns
	// false when the connection is closing (write not queued).
	bool WriteReply(const Reply &reply);
	// Drains the outbound queue, one async_write in flight at a time.
	void DoWrite();

	Server &server_;
	asio::local::stream_protocol::socket socket_;
	asio::streambuf buf_;
	// Outbound frame queue: at most one async_write is in flight at a time
	// (concurrent async_write on one socket is undefined behaviour).
	std::deque<std::shared_ptr<std::string>> write_queue_;
	bool writing_ {false};
	// Follows the http.cpp idiom: shared flag so async handlers can detect that the
	// connection was torn down out from under them and bail without touching freed state.
	std::shared_ptr<bool> cancelled_;
	log::Logger logger_;
};

using ConnectionPtr = std::shared_ptr<Connection>;

// A hand-rolled varlink server over a Boost.Asio unix stream socket. Messages are
// JSON objects separated by single NUL bytes.
class Server : public events::EventLoopObject {
public:
	Server(events::EventLoop &loop);
	~Server();

	Server(const Server &) = delete;
	Server &operator=(const Server &) = delete;

	void RegisterMethod(const std::string &method, MethodHandler handler);

	// Service metadata reported by the standard org.varlink.service.GetInfo call,
	// so any varlink client (e.g. `varlinkctl info`) can introspect the service.
	void SetServiceInfo(
		const std::string &vendor,
		const std::string &product,
		const std::string &version,
		const std::string &url);
	// Publish a varlink interface's IDL description (returned by
	// org.varlink.service.GetInterfaceDescription and listed by GetInfo).
	void AddInterface(const std::string &name, const std::string &description);

	error::Error Listen(const std::string &socket_path);

	void Stop();

private:
	void AsyncAccept();
	void RemoveConnection(const ConnectionPtr &conn);
	// Registers the standard org.varlink.service.{GetInfo,GetInterfaceDescription}
	// methods. Called from Listen() after the app has registered its own methods.
	void RegisterServiceInterface();

	// Returns the handler registered for the fully-qualified method, or nullptr.
	const MethodHandler *LookupMethod(const std::string &method) const;

	events::EventLoop &loop_;
	asio::io_context &ctx_;
	asio::local::stream_protocol::acceptor acceptor_;
	std::set<ConnectionPtr> connections_;
	std::unordered_map<std::string, MethodHandler> methods_;
	std::string socket_path_;
	std::string svc_vendor_ {"Northern.tech"};
	std::string svc_product_;
	std::string svc_version_;
	std::string svc_url_;
	std::map<std::string, std::string> interface_descriptions_;
	log::Logger logger_;

	friend class Connection;
};

} // namespace varlink
} // namespace common
} // namespace mender

#endif // MENDER_COMMON_VARLINK_SERVER_HPP
