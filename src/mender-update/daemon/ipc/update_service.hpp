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

#ifndef MENDER_UPDATE_DAEMON_IPC_UPDATE_SERVICE_HPP
#define MENDER_UPDATE_DAEMON_IPC_UPDATE_SERVICE_HPP

#include <string>

#include <common/error.hpp>
#include <common/events.hpp>
#include <common/varlink/server.hpp>
#include <mender-update/daemon/context.hpp>

namespace mender {
namespace update {
namespace daemon {

// Forward declaration to avoid circular include with state_machine.hpp.
class StateMachine;

namespace ipc {

namespace error = mender::common::error;
namespace events = mender::common::events;
namespace varlink = mender::common::varlink;

using namespace std;

class UpdateService {
public:
	UpdateService(events::EventLoop &loop, Context &ctx, StateMachine &sm);
	error::Error Listen(const string &socket_path);
	void Stop();

private:
	Context &ctx_;
	StateMachine &sm_;
	varlink::Server server_;
};

} // namespace ipc
} // namespace daemon
} // namespace update
} // namespace mender

#endif // MENDER_UPDATE_DAEMON_IPC_UPDATE_SERVICE_HPP
