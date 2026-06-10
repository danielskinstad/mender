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

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include <boost/asio.hpp>

#include <client_shared/conf.hpp>
#include <common/error.hpp>
#include <common/events.hpp>
#include <common/json.hpp>
#include <common/path.hpp>
#include <common/testing.hpp>
#include <common/varlink/varlink.hpp>

#include <mender-update/context.hpp>
#include <mender-update/daemon/context.hpp>
#include <mender-update/daemon/state_machine.hpp>
#include <mender-update/daemon/ipc/update_service.hpp>

namespace mender {
namespace update {
namespace daemon {
namespace ipc {

namespace asio = boost::asio;
namespace conf = mender::client_shared::conf;
namespace context = mender::update::context;
namespace error = mender::common::error;
namespace events = mender::common::events;
namespace json = mender::common::json;
namespace path = mender::common::path;
namespace mtesting = mender::common::testing;
namespace varlink = mender::common::varlink;

using namespace std;

// Exposes the io_context backing an EventLoop so tests can create raw client sockets.
struct IoCtxAccess : public events::EventLoopObject {
	static asio::io_context &Get(events::EventLoop &l) {
		return GetAsioIoContext(l);
	}
};

namespace {

// Raw varlink test client: connect, write one frame, read one frame.
class TestClient : public std::enable_shared_from_this<TestClient> {
public:
	TestClient(asio::io_context &ctx, const string &path) :
		socket_ {ctx},
		path_ {path} {
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

	void Close() {
		boost::system::error_code ec;
		socket_.close(ec);
	}

private:
	asio::local::stream_protocol::socket socket_;
	asio::streambuf buf_;
	string path_;
};

} // namespace

TEST(UpdateServiceTest, ListenOnTempSocket) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	events::EventLoop event_loop;
	Context ctx(main_context, event_loop);
	StateMachine state_machine(ctx, event_loop);

	UpdateService service(event_loop, ctx, state_machine);

	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	EXPECT_EQ(serr, error::NoError) << serr.String();

	service.Stop();
}

TEST(UpdateServiceTest, GetStatePausedDeployment) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// Fake a paused deployment.
	ctx.deployment.state_data = make_unique<StateData>();
	ctx.deployment.state_data->update_info.id = "dep-1";
	ctx.deployment.state_data->update_info.pause_deadline = 1234567890;
	ctx.deployment.paused = true;
	ctx.deployment.paused_state = "ArtifactInstall";

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.GetState"})" + string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					auto params = exp.value().Get("parameters");
					ASSERT_TRUE(params) << "missing 'parameters' key";

					auto paused = params.value().Get("paused");
					ASSERT_TRUE(paused);
					auto paused_bool = paused.value().GetBool();
					ASSERT_TRUE(paused_bool);
					EXPECT_TRUE(paused_bool.value());

					auto state = params.value().Get("state");
					ASSERT_TRUE(state);
					auto state_str = state.value().GetString();
					ASSERT_TRUE(state_str);
					EXPECT_EQ(state_str.value(), "ArtifactInstall");

					auto status = params.value().Get("status");
					ASSERT_TRUE(status);
					auto status_str = status.value().GetString();
					ASSERT_TRUE(status_str);
					EXPECT_EQ(status_str.value(), "paused");

					auto dep_id = params.value().Get("deployment_id");
					ASSERT_TRUE(dep_id);
					auto dep_id_str = dep_id.value().GetString();
					ASSERT_TRUE(dep_id_str);
					EXPECT_EQ(dep_id_str.value(), "dep-1");

					loop.Stop();
				});
			});
	});

	loop.Run();
}

TEST(UpdateServiceTest, GetStateIdle) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// No deployment active — state_data is null by default.

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.GetState"})" + string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					auto params = exp.value().Get("parameters");
					ASSERT_TRUE(params) << "missing 'parameters' key";

					auto paused = params.value().Get("paused");
					ASSERT_TRUE(paused);
					auto paused_bool = paused.value().GetBool();
					ASSERT_TRUE(paused_bool);
					EXPECT_FALSE(paused_bool.value());

					auto status = params.value().Get("status");
					ASSERT_TRUE(status);
					auto status_str = status.value().GetString();
					ASSERT_TRUE(status_str);
					EXPECT_EQ(status_str.value(), "idle");

					auto dep_id = params.value().Get("deployment_id");
					ASSERT_TRUE(dep_id);
					auto dep_id_str = dep_id.value().GetString();
					ASSERT_TRUE(dep_id_str);
					EXPECT_EQ(dep_id_str.value(), "");

					loop.Stop();
				});
			});
	});

	loop.Run();
}

// The standard org.varlink.service introspection interface must work, so generic
// varlink clients (varlinkctl, libvarlink) can discover io.mender.Update1.
TEST(UpdateServiceTest, GetInfoListsInterface) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());
	context::MenderContext main_context(config);
	ASSERT_EQ(main_context.Initialize(), error::NoError);

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);
	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	ASSERT_EQ(service.Listen(sock_path), error::NoError);

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"org.varlink.service.GetInfo"})" + string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();
					auto params = exp.value().Get("parameters");
					ASSERT_TRUE(params);
					auto product = params.value().Get("product").and_then([](const json::Json &j) {
						return j.GetString();
					});
					ASSERT_TRUE(product);
					EXPECT_EQ(product.value(), "mender-update");
					// interfaces array contains io.mender.Update1
					auto ifaces = params.value().Get("interfaces");
					ASSERT_TRUE(ifaces);
					bool found = false;
					auto n = ifaces.value().GetArraySize();
					ASSERT_TRUE(n);
					for (size_t i = 0; i < n.value(); i++) {
						auto s = ifaces.value().Get(i).and_then([](const json::Json &j) {
							return j.GetString();
						});
						if (s && s.value() == "io.mender.Update1") {
							found = true;
						}
					}
					EXPECT_TRUE(found) << "io.mender.Update1 not listed by GetInfo";
					loop.Stop();
				});
			});
	});
	loop.Run();
}

TEST(UpdateServiceTest, GetInterfaceDescriptionReturnsIdl) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());
	context::MenderContext main_context(config);
	ASSERT_EQ(main_context.Initialize(), error::NoError);

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);
	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	ASSERT_EQ(service.Listen(sock_path), error::NoError);

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"org.varlink.service.GetInterfaceDescription","parameters":{"interface":"io.mender.Update1"}})"
				+ string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();
					auto desc = exp.value()
									.Get("parameters")
									.and_then([](const json::Json &j) { return j.Get("description"); })
									.and_then([](const json::Json &j) { return j.GetString(); });
					ASSERT_TRUE(desc);
					EXPECT_NE(desc.value().find("interface io.mender.Update1"), string::npos);
					EXPECT_NE(desc.value().find("method GetState"), string::npos);
					loop.Stop();
				});
			});
	});
	loop.Run();
}

TEST(StateMachineControlTest, ResumeNotPaused) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// No deployment paused.
	auto cerr = state_machine.ResumePausedDeployment();
	EXPECT_NE(cerr, error::NoError);
	EXPECT_EQ(cerr.code, error_condition(NotPausedError, ControlErrorCategory));
}

TEST(StateMachineControlTest, AbortNotPaused) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	auto cerr = state_machine.AbortPausedDeployment();
	EXPECT_NE(cerr, error::NoError);
	EXPECT_EQ(cerr.code, error_condition(NotPausedError, ControlErrorCategory));
}

TEST(StateMachineControlTest, ExtendNotPaused) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	auto cerr = state_machine.ExtendPauseTimeout(3600);
	EXPECT_NE(cerr, error::NoError);
	EXPECT_EQ(cerr.code, error_condition(NotPausedError, ControlErrorCategory));
}

TEST(StateMachineControlTest, ExtendInvalidArgument) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// Fake a paused deployment so the guard passes and we hit the arg check.
	ctx.deployment.state_data = make_unique<StateData>();
	ctx.deployment.paused = true;

	auto cerr = state_machine.ExtendPauseTimeout(0);
	EXPECT_NE(cerr, error::NoError);
	EXPECT_EQ(cerr.code, error_condition(InvalidArgumentError, ControlErrorCategory));
}

TEST(StateMachineControlTest, ExtendSucceeds) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// Fake a paused deployment with state data persisted to the store.
	ctx.deployment.state_data = make_unique<StateData>();
	ctx.deployment.state_data->update_info.id = "dep-extend";
	ctx.deployment.state_data->update_info.pause_deadline = 0;
	ctx.deployment.paused = true;
	ctx.deployment.paused_state = "ArtifactInstall";

	auto cerr = state_machine.ExtendPauseTimeout(3600);
	EXPECT_EQ(cerr, error::NoError) << cerr.String();

	// Deadline should have moved to roughly now+3600 (a large epoch value).
	EXPECT_GT(ctx.deployment.state_data->update_info.pause_deadline, 1000000000);

	// Cancel the armed timer for clean teardown.
	ctx.pause_timer.Cancel();
}

TEST(UpdateServiceTest, ContinueWhenNotPausedReturnsNotPaused) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// No deployment paused.

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.Continue"})" + string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					auto err_field = exp.value().Get("error");
					ASSERT_TRUE(err_field) << "expected 'error' field in reply";
					auto err_str = err_field.value().GetString();
					ASSERT_TRUE(err_str);
					EXPECT_EQ(err_str.value(), "io.mender.Update1.NotPaused");

					loop.Stop();
				});
			});
	});

	loop.Run();
}

TEST(UpdateServiceTest, AbortWhenNotPausedReturnsNotPaused) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// No deployment paused.

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.Abort"})" + string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					auto err_field = exp.value().Get("error");
					ASSERT_TRUE(err_field) << "expected 'error' field in reply";
					auto err_str = err_field.value().GetString();
					ASSERT_TRUE(err_str);
					EXPECT_EQ(err_str.value(), "io.mender.Update1.NotPaused");

					loop.Stop();
				});
			});
	});

	loop.Run();
}

// Tests that Continue (via the SM control method) clears the paused flag and
// returns NoError when a deployment is actually paused. We test this at the SM
// level (not through the socket) because the SM is in IdleState during the unit
// test: posting Success to it through the event loop would trigger a fatal abort.
// The varlink error-mapping path (NoError → empty reply) is implicitly verified
// by ExtendTimeoutIncreasesDeadline below.
TEST(StateMachineControlTest, ResumeWhenPausedClearsFlagAndReturnsNoError) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// Fake a paused deployment.
	ctx.deployment.state_data = make_unique<StateData>();
	ctx.deployment.state_data->update_info.id = "dep-continue";
	ctx.deployment.paused = true;
	ctx.deployment.paused_state = "ArtifactInstall";

	// Call Resume directly (not via socket) so the Success event is posted but
	// loop.Run() is never called, preventing the fatal RunOne assertion.
	auto cerr = state_machine.ResumePausedDeployment();
	EXPECT_EQ(cerr, error::NoError) << cerr.String();
	EXPECT_FALSE(ctx.deployment.paused);

	// Cancel timers for clean teardown.
	ctx.pause_timer.Cancel();
	ctx.pause_inventory_timer.Cancel();
}

TEST(UpdateServiceTest, ExtendTimeoutIncreasesDeadline) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// Fake a paused deployment.
	ctx.deployment.state_data = make_unique<StateData>();
	ctx.deployment.state_data->update_info.id = "dep-extend";
	ctx.deployment.state_data->update_info.pause_deadline = 0;
	ctx.deployment.paused = true;
	ctx.deployment.paused_state = "ArtifactInstall";

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.ExtendTimeout","parameters":{"seconds":3600}})"
				+ string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					// No 'error' field means success.
					auto err_field = exp.value().Get("error");
					EXPECT_FALSE(err_field) << "unexpected error: "
											<< err_field.value().GetString().value_or("?");

					loop.Stop();
				});
			});
	});

	loop.Run();

	// Deadline should have been extended to roughly now+3600.
	EXPECT_GT(ctx.deployment.state_data->update_info.pause_deadline, 1000000000);

	// Cancel timers for clean teardown.
	ctx.pause_timer.Cancel();
	ctx.pause_inventory_timer.Cancel();
}

TEST(UpdateServiceTest, ExtendTimeoutMissingSecondsReturnsInvalidParameter) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.ExtendTimeout"})" + string {varlink::kMessageSeparator},
			[&loop, client]() {
				client->ReadFrame([&loop](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					auto err_field = exp.value().Get("error");
					ASSERT_TRUE(err_field) << "expected 'error' field in reply";
					auto err_str = err_field.value().GetString();
					ASSERT_TRUE(err_str);
					EXPECT_EQ(err_str.value(), "io.mender.Update1.InvalidParameter");

					loop.Stop();
				});
			});
	});

	loop.Run();
}

TEST(UpdateServiceTest, MonitorStreamsStatusChanges) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	// No deployment active initially — first frame should report idle.

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, &ctx, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.Monitor","more":true})"
				+ string {varlink::kMessageSeparator},
			[&loop, &ctx, client]() {
				// Frame 1: current snapshot (idle), with continues=true.
				client->ReadFrame([&loop, &ctx, client](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					auto continues = exp.value().Get("continues");
					ASSERT_TRUE(continues) << "expected 'continues' field on streamed frame";
					EXPECT_TRUE(continues.value().GetBool().value_or(false));

					auto params = exp.value().Get("parameters");
					ASSERT_TRUE(params) << "missing 'parameters' key";
					auto status = params.value().Get("status").and_then([](const json::Json &j) {
						return j.GetString();
					});
					ASSERT_TRUE(status);
					EXPECT_EQ(status.value(), "idle");

					// Mutate state to paused and notify; should produce frame 2.
					ctx.deployment.state_data = make_unique<StateData>();
					ctx.deployment.state_data->update_info.id = "dep-monitor";
					ctx.deployment.paused = true;
					ctx.deployment.paused_state = "ArtifactInstall";
					ctx.NotifyStatusChanged();

					// Frame 2: paused snapshot.
					client->ReadFrame([&loop](const string &frame2) {
						auto exp2 = json::Load(frame2);
						ASSERT_TRUE(exp2) << exp2.error().String();

						auto params2 = exp2.value().Get("parameters");
						ASSERT_TRUE(params2) << "missing 'parameters' key";

						auto paused =
							params2.value().Get("paused").and_then([](const json::Json &j) {
								return j.GetBool();
							});
						ASSERT_TRUE(paused);
						EXPECT_TRUE(paused.value());

						auto state =
							params2.value().Get("state").and_then([](const json::Json &j) {
								return j.GetString();
							});
						ASSERT_TRUE(state);
						EXPECT_EQ(state.value(), "ArtifactInstall");

						loop.Stop();
					});
				});
			});
	});

	loop.Run();
}

// Monitor without `more` must return a single snapshot (continues=false) and
// not subscribe -- the client did not ask for a stream.
TEST(UpdateServiceTest, MonitorWithoutMoreReturnsSingleSnapshot) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	ASSERT_EQ(main_context.Initialize(), error::NoError);

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	ASSERT_EQ(service.Listen(sock_path), error::NoError);

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, &ctx, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.Monitor"})" + string {varlink::kMessageSeparator},
			[&loop, &ctx, client]() {
				client->ReadFrame([&loop, &ctx](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();
					// Not a stream: continues must be absent or false.
					auto continues = exp.value().Get("continues");
					EXPECT_FALSE(continues && continues.value().GetBool().value_or(false));
					// And no observer was registered.
					EXPECT_EQ(ctx.StatusObserverCount(), 0u);
					loop.Stop();
				});
			});
	});

	loop.Run();
}

TEST(UpdateServiceTest, MonitorPrunesObserverWhenClientDisconnects) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, &ctx, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.Monitor","more":true})"
				+ string {varlink::kMessageSeparator},
			[&loop, &ctx, client]() {
				client->ReadFrame([&loop, &ctx, client](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					// The observer should now be registered.
					EXPECT_EQ(ctx.StatusObserverCount(), 1u);

					// Close the client; the server detects EOF asynchronously and
					// closes its end. A subsequent notify finds the connection
					// cancelled, emit() returns false, and the observer is pruned.
					client->Close();

					// Give the server's read handler a chance to observe EOF, then
					// notify twice. WriteReply returns false once the connection is
					// cancelled, so the observer is removed.
					auto timer = make_shared<events::Timer>(loop);
					timer->AsyncWait(
						chrono::milliseconds(200),
						[&loop, &ctx, timer](error::Error) {
							ctx.NotifyStatusChanged();
							ctx.NotifyStatusChanged();
							EXPECT_EQ(ctx.StatusObserverCount(), 0u);
							loop.Stop();
						});
				});
			});
	});

	loop.Run();
}

// Drives the daemon-facing contract directly: ReportDownloadProgress should fire
// status observers once per *distinct* percentage increase, with the right
// download_progress in the snapshot. Duplicate values are coalesced.
TEST(UpdateServiceTest, ReportDownloadProgressNotifiesObserversOnDistinctIncrease) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);

	vector<int> seen;
	ctx.AddStatusObserver([&seen](const DeploymentStatusSnapshot &s) {
		seen.push_back(s.download_progress);
		return true; // keep the observer registered
	});

	ctx.ReportDownloadProgress(10);
	ctx.ReportDownloadProgress(10); // duplicate, no-op
	ctx.ReportDownloadProgress(42);
	ctx.ReportDownloadProgress(42); // duplicate, no-op
	ctx.ReportDownloadProgress(100);

	ASSERT_EQ(seen.size(), 3u);
	EXPECT_EQ(seen[0], 10);
	EXPECT_EQ(seen[1], 42);
	EXPECT_EQ(seen[2], 100);

	// The snapshot reflects the last reported value.
	EXPECT_EQ(ctx.BuildStatusSnapshot().download_progress, 100);
}

// Monitor frames must expose download_progress so subscribers get a live feed.
TEST(UpdateServiceTest, MonitorStreamsDownloadProgress) {
	mtesting::TemporaryDirectory tmpdir;
	conf::MenderConfig config;
	config.paths.SetDataStore(tmpdir.Path());

	context::MenderContext main_context(config);
	auto err = main_context.Initialize();
	ASSERT_EQ(err, error::NoError) << err.String();

	mtesting::TestEventLoop loop;
	Context ctx(main_context, loop);
	StateMachine state_machine(ctx, loop);

	UpdateService service(loop, ctx, state_machine);
	const string sock_path = path::Join(tmpdir.Path(), "update.sock");
	auto serr = service.Listen(sock_path);
	ASSERT_EQ(serr, error::NoError) << serr.String();

	auto client = make_shared<TestClient>(IoCtxAccess::Get(loop), sock_path);
	client->Connect([&loop, &ctx, client]() {
		client->Write(
			R"({"method":"io.mender.Update1.Monitor","more":true})"
				+ string {varlink::kMessageSeparator},
			[&loop, &ctx, client]() {
				// Frame 1: idle snapshot, download_progress = -1.
				client->ReadFrame([&loop, &ctx, client](const string &frame) {
					auto exp = json::Load(frame);
					ASSERT_TRUE(exp) << exp.error().String();

					auto params = exp.value().Get("parameters");
					ASSERT_TRUE(params) << "missing 'parameters' key";
					auto dp = params.value().Get("download_progress").and_then([](const json::Json &j) {
						return j.GetInt64();
					});
					ASSERT_TRUE(dp) << "missing 'download_progress' field";
					EXPECT_EQ(dp.value(), -1);

					// Report progress; should produce frame 2 with download_progress=37.
					ctx.ReportDownloadProgress(37);

					client->ReadFrame([&loop](const string &frame2) {
						auto exp2 = json::Load(frame2);
						ASSERT_TRUE(exp2) << exp2.error().String();

						auto params2 = exp2.value().Get("parameters");
						ASSERT_TRUE(params2) << "missing 'parameters' key";
						auto dp2 =
							params2.value().Get("download_progress").and_then([](const json::Json &j) {
								return j.GetInt64();
							});
						ASSERT_TRUE(dp2);
						EXPECT_EQ(dp2.value(), 37);

						loop.Stop();
					});
				});
			});
	});

	loop.Run();
}

} // namespace ipc
} // namespace daemon
} // namespace update
} // namespace mender
