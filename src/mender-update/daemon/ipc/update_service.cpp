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

#include <mender-update/daemon/ipc/update_service.hpp>

#include <mender-update/daemon/state_machine.hpp>

#include <nlohmann/json.hpp>

#include <client_shared/conf.hpp>
#include <common/json.hpp>

namespace mender {
namespace update {
namespace daemon {
namespace ipc {

namespace json = mender::common::json;
namespace conf = mender::client_shared::conf;

// Serialize a deployment status snapshot into a varlink Reply. `continues`
// controls whether the client should keep listening for more frames (used by
// the streaming Monitor method).
static varlink::Reply SnapshotToReply(const DeploymentStatusSnapshot &s, bool continues) {
	nlohmann::json p;
	p["paused"] = s.paused;
	p["state"] = s.paused_state;
	p["status"] = s.paused ? "paused" : (s.active ? "in-progress" : "idle");
	p["deployment_id"] = s.deployment_id;
	p["pause_deadline"] = s.pause_deadline;
	p["download_progress"] = s.download_progress;
	varlink::Reply r;
	r.parameters_json = p.dump();
	r.continues = continues;
	return r;
}

// Map a StateMachine control error to a varlink Reply.
static varlink::Reply ControlErrToReply(const error::Error &err) {
	varlink::Reply r;
	if (err == error::NoError) {
		return r; // empty success reply {}
	}
	if (err.code == MakeError(NotPausedError, "").code) {
		r.error_name = "io.mender.Update1.NotPaused";
	} else if (err.code == MakeError(InvalidArgumentError, "").code) {
		r.error_name = "io.mender.Update1.InvalidParameter";
	} else {
		r.error_name = "io.mender.Update1.InternalError";
		nlohmann::json p;
		p["message"] = err.String();
		r.parameters_json = p.dump();
	}
	return r;
}

UpdateService::UpdateService(events::EventLoop &loop, Context &ctx, StateMachine &sm) :
	ctx_(ctx),
	sm_(sm),
	server_(loop) {
}

// varlink IDL for the io.mender.Update1 interface, returned by
// org.varlink.service.GetInterfaceDescription / introspected by varlink clients.
static const char *kUpdate1Idl =
	"interface io.mender.Update1\n"
	"\n"
	"# Current deployment / pause state.\n"
	"method GetState() -> (\n"
	"  status: string,\n"
	"  state: string,\n"
	"  paused: bool,\n"
	"  deployment_id: string,\n"
	"  pause_deadline: int,\n"
	"  download_progress: int\n"
	")\n"
	"\n"
	"# Resume a paused deployment.\n"
	"method Continue() -> ()\n"
	"\n"
	"# Abort and roll back a paused deployment.\n"
	"method Abort() -> ()\n"
	"\n"
	"# Extend the pause timeout.\n"
	"method ExtendTimeout(seconds: int) -> ()\n"
	"\n"
	"# Stream status changes and download-progress updates (call with 'more').\n"
	"method Monitor() -> (\n"
	"  status: string,\n"
	"  state: string,\n"
	"  paused: bool,\n"
	"  deployment_id: string,\n"
	"  pause_deadline: int,\n"
	"  download_progress: int\n"
	")\n"
	"\n"
	"error NotPaused()\n"
	"error InvalidParameter()\n";

error::Error UpdateService::Listen(const string &socket_path) {
	server_.SetServiceInfo("Northern.tech", "mender-update", conf::kMenderVersion, "");
	server_.AddInterface("io.mender.Update1", kUpdate1Idl);

	server_.RegisterMethod(
		"io.mender.Update1.GetState",
		[this](const varlink::MethodCall &, const varlink::Replier &) {
			return SnapshotToReply(sm_.QueryDeploymentState(), /*continues=*/false);
		});
	server_.RegisterMethod(
		"io.mender.Update1.Monitor",
		[this](const varlink::MethodCall &call, const varlink::Replier &emit) {
			// Monitor is a streaming method: per the varlink protocol it must be
			// called with `more`. Without it, return a single current snapshot and
			// do not subscribe (so the client isn't sent an unexpected stream).
			if (!call.more) {
				return SnapshotToReply(sm_.QueryDeploymentState(), /*continues=*/false);
			}
			// Stream every future status change to this connection. The observer
			// holds `emit`, which weakly references the connection; when the client
			// disconnects, emit() returns false and NotifyStatusChanged prunes it.
			ctx_.AddStatusObserver([emit](const DeploymentStatusSnapshot &s) -> bool {
				return emit(SnapshotToReply(s, /*continues=*/true));
			});
			// Terminal reply = the current snapshot, with continues=true so the client
			// keeps the stream open for subsequent signals.
			return SnapshotToReply(sm_.QueryDeploymentState(), /*continues=*/true);
		});
	server_.RegisterMethod(
		"io.mender.Update1.Continue",
		[this](const varlink::MethodCall &, const varlink::Replier &) {
			return ControlErrToReply(sm_.ResumePausedDeployment());
		});
	server_.RegisterMethod(
		"io.mender.Update1.Abort",
		[this](const varlink::MethodCall &, const varlink::Replier &) {
			return ControlErrToReply(sm_.AbortPausedDeployment());
		});
	server_.RegisterMethod(
		"io.mender.Update1.ExtendTimeout",
		[this](const varlink::MethodCall &call, const varlink::Replier &) {
			auto secs = call.parameters.Get("seconds").and_then(json::ToInt64);
			if (!secs) {
				varlink::Reply r;
				r.error_name = "io.mender.Update1.InvalidParameter";
				return r;
			}
			return ControlErrToReply(sm_.ExtendPauseTimeout(secs.value()));
		});
	return server_.Listen(socket_path);
}

void UpdateService::Stop() {
	server_.Stop();
}

} // namespace ipc
} // namespace daemon
} // namespace update
} // namespace mender
