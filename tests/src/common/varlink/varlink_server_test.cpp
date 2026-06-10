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

#include <memory>
#include <string>

#include <boost/asio.hpp>
#include <gtest/gtest.h>

#include <common/events.hpp>
#include <common/json.hpp>
#include <common/testing.hpp>

using namespace std;

namespace asio = boost::asio;
namespace error = mender::common::error;
namespace events = mender::common::events;
namespace json = mender::common::json;
namespace mtesting = mender::common::testing;
namespace varlink = mender::common::varlink;

using TestEventLoop = mtesting::TestEventLoop;

// Exposes the io_context backing an EventLoop so the test can create a raw client
// socket on the same loop.
struct IoCtxAccess : public events::EventLoopObject {
	static asio::io_context &Get(events::EventLoop &l) {
		return GetAsioIoContext(l);
	}
};

namespace {

// Helper holding a client socket + read buffer that connects to the server and
// runs a single call/read interaction on the loop.
class TestClient : public std::enable_shared_from_this<TestClient> {
public:
	TestClient(asio::io_context &ctx, const string &path) :
		socket_ {ctx},
		path_ {path} {
	}

	asio::local::stream_protocol::socket &Socket() {
		return socket_;
	}

	void Connect(function<void()> on_connected) {
		auto self = shared_from_this();
		socket_.async_connect(
			asio::local::stream_protocol::endpoint {path_},
			[self, on_connected](const boost::system::error_code &ec) {
				ASSERT_FALSE(ec) << ec.message();
				on_connected();
			});
	}

	void Write(const string &frame, function<void()> on_written) {
		auto self = shared_from_this();
		auto data = make_shared<string>(frame);
		asio::async_write(
			socket_,
			asio::buffer(*data),
			[self, data, on_written](const boost::system::error_code &ec, size_t) {
				ASSERT_FALSE(ec) << ec.message();
				on_written();
			});
	}

	void ReadFrame(function<void(const string &)> on_frame) {
		auto self = shared_from_this();
		asio::async_read_until(
			socket_,
			buf_,
			varlink::kMessageSeparator,
			[self, on_frame](const boost::system::error_code &ec, size_t) {
				ASSERT_FALSE(ec) << ec.message();
				std::istream is(&self->buf_);
				string frame;
				std::getline(is, frame, varlink::kMessageSeparator);
				on_frame(frame);
			});
	}

private:
	asio::local::stream_protocol::socket socket_;
	asio::streambuf buf_;
	string path_;
};

} // namespace

TEST(VarlinkServer, DispatchesMethodAndReplies) {
	TestEventLoop loop;
	mtesting::TemporaryDirectory tmpdir;
	string socket_path = tmpdir.Path() + "/varlink.sock";

	varlink::Server server {loop};
	server.RegisterMethod(
		"io.test.Echo", [](const varlink::MethodCall &call, const varlink::Replier &) {
			varlink::Reply reply;
			reply.parameters_json = call.parameters.Dump();
			return reply;
		});

	ASSERT_EQ(server.Listen(socket_path), error::NoError);

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), socket_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"io.test.Echo","parameters":{"hello":"world"}})"
				+ string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();
					auto params = exp.value().Get("parameters");
					ASSERT_TRUE(params);
					auto hello = params.value().Get("hello");
					ASSERT_TRUE(hello);
					auto str = hello.value().GetString();
					ASSERT_TRUE(str);
					EXPECT_EQ(str.value(), "world");
					loop.Stop();
				});
			});
	});

	loop.Run();
}

TEST(VarlinkServer, UnknownMethodReturnsError) {
	TestEventLoop loop;
	mtesting::TemporaryDirectory tmpdir;
	string socket_path = tmpdir.Path() + "/varlink.sock";

	varlink::Server server {loop};

	ASSERT_EQ(server.Listen(socket_path), error::NoError);

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), socket_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"io.test.DoesNotExist","parameters":{}})"
				+ string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();
					auto err = exp.value().Get("error");
					ASSERT_TRUE(err);
					auto str = err.value().GetString();
					ASSERT_TRUE(str);
					EXPECT_EQ(str.value(), "org.varlink.service.MethodNotFound");
					loop.Stop();
				});
			});
	});

	loop.Run();
}

TEST(VarlinkServer, MalformedJsonReturnsError) {
	TestEventLoop loop;
	mtesting::TemporaryDirectory tmpdir;
	string socket_path = tmpdir.Path() + "/varlink.sock";

	varlink::Server server {loop};

	ASSERT_EQ(server.Listen(socket_path), error::NoError);

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), socket_path);
	client->Connect([&loop, client]() {
		client->Write(
			"not json" + string {varlink::kMessageSeparator}, [&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();
					auto err = exp.value().Get("error");
					EXPECT_TRUE(err);
					loop.Stop();
				});
			});
	});

	loop.Run();
}

TEST(VarlinkServer, OnewayGetsNoReply) {
	TestEventLoop loop;
	mtesting::TemporaryDirectory tmpdir;
	string socket_path = tmpdir.Path() + "/varlink.sock";

	bool handler_called = false;

	varlink::Server server {loop};
	server.RegisterMethod(
		"io.test.Fire",
		[&handler_called](const varlink::MethodCall &, const varlink::Replier &) {
			handler_called = true;
			return varlink::Reply {};
		});

	ASSERT_EQ(server.Listen(socket_path), error::NoError);

	bool got_reply = false;
	auto timer = make_shared<events::Timer>(loop);

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), socket_path);
	client->Connect([&loop, client, &got_reply, &handler_called, timer]() {
		client->Write(
			R"({"method":"io.test.Fire","parameters":{},"oneway":true})"
				+ string {varlink::kMessageSeparator},
			[&loop, client, &got_reply, &handler_called, timer]() {
				// Start a read that should never complete with data.
				client->ReadFrame([&got_reply](const string &) {
					got_reply = true;
				});
				// After a short delay, assert no reply arrived, then stop.
				timer->AsyncWait(chrono::milliseconds(300), [&loop, &got_reply, &handler_called](error::Error) {
					EXPECT_FALSE(got_reply);
					EXPECT_TRUE(handler_called);
					loop.Stop();
				});
			});
	});

	loop.Run();
}
