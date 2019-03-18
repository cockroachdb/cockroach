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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// CancelRequestCounter is to be incremented every time a pgwire-level
// cancel request is received from a client.
var CancelRequestCounter = telemetry.GetCounterOnce("pgwire.unimplemented.cancel_request")

// UnimplementedClientStatusParameterCounter is to be incremented
// every time a client attempts to configure a status parameter
// that's not supported upon session initialization.
func UnimplementedClientStatusParameterCounter(key string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("unimplemented.pgwire.parameter.%s", key))
}

// BinaryDecimalInfinityCounter is to be incremented every time a
// client requests the binary encoding for a decimal infinity, which
// is not well defined in the pg protocol (#32489).
var BinaryDecimalInfinityCounter = telemetry.GetCounterOnce("pgwire.#32489.binary_decimal_infinity")

// UncategorizedErrorCounter is to be incremented every time an error
// flows to the client without having been decorated with a pg error.
var UncategorizedErrorCounter = telemetry.GetCounterOnce("othererror." + pgerror.CodeUncategorizedError)
