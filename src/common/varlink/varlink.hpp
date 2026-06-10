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

#ifndef MENDER_COMMON_VARLINK_HPP
#define MENDER_COMMON_VARLINK_HPP

#include <functional>
#include <string>

#include <common/error.hpp>
#include <common/expected.hpp>
#include <common/json.hpp>

namespace mender {
namespace common {
namespace varlink {

namespace error = mender::common::error;
namespace expected = mender::common::expected;
namespace json = mender::common::json;

using namespace std;

constexpr char kMessageSeparator = '\0';

enum VarlinkErrorCode {
	NoError = 0,
	ProtocolError,
	TransportError,
};
class ErrorCategoryClass : public std::error_category {
public:
	const char *name() const noexcept override;
	string message(int code) const override;
};
extern const ErrorCategoryClass ErrorCategory;
error::Error MakeError(VarlinkErrorCode code, const string &msg);

struct MethodCall {
	string method;
	json::Json parameters;
	bool more {false};
	bool oneway {false};
};

struct Reply {
	string parameters_json {"{}"}; // JSON object string for "parameters"
	bool continues {false};
	string error_name;             // non-empty => error reply
};

using Replier = function<bool(const Reply &)>;
using MethodHandler = function<Reply(const MethodCall &call, const Replier &emit)>;

} // namespace varlink
} // namespace common
} // namespace mender
#endif
