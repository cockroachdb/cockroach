// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

// DefaultIntSize4Counter is to be incremented every time a client
// change the default_int_size variable to its non-default value 4.
var DefaultIntSize4Counter = telemetry.GetCounterOnce("sql.default_int_size.4")

// ForceSavepointRestartCounter is to be incremented every time a
// client customizes the session variable force_savepoint_restart
// to a non-empty string.
var ForceSavepointRestartCounter = telemetry.GetCounterOnce("sql.force_savepoint_restart")

// UnimplementedSessionVarValueCounter is to be incremented every time
// a client attempts to set a compatitibility session var to an
// unsupported value.
func UnimplementedSessionVarValueCounter(varName, val string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("unimplemented.sql.session_var.%s.%s", varName, val))
}
