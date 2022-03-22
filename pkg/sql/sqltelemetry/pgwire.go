// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
)

// CancelRequestCounter is to be incremented every time a pgwire-level
// cancel request is received from a client.
var CancelRequestCounter = telemetry.GetCounterOnce("pgwire.cancel_request")

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
var UncategorizedErrorCounter = telemetry.GetCounterOnce("othererror." + pgcode.Uncategorized.String())

// InterleavedPortalRequestCounter is to be incremented every time an open
// portal attempts to interleave work with another portal.
var InterleavedPortalRequestCounter = telemetry.GetCounterOnce("pgwire.#40195.interleaved_portal")

// PortalWithLimitRequestCounter is to be incremented every time a portal request is
// made.
var PortalWithLimitRequestCounter = telemetry.GetCounterOnce("pgwire.portal_with_limit_request")

// ParseRequestCounter is to be incremented every time a parse request
// is made.
var ParseRequestCounter = telemetry.GetCounterOnce("pgwire.command.parse")

// BindRequestCounter is to be incremented every time a bind request
// is made.
var BindRequestCounter = telemetry.GetCounterOnce("pgwire.command.bind")

// DescribeRequestCounter is to be incremented every time a describe request
// is made.
var DescribeRequestCounter = telemetry.GetCounterOnce("pgwire.command.describe")

// ExecuteRequestCounter is to be incremented every time a execute request
// is made.
var ExecuteRequestCounter = telemetry.GetCounterOnce("pgwire.command.execute")

// CloseRequestCounter is to be incremented every time a close request
// is made.
var CloseRequestCounter = telemetry.GetCounterOnce("pgwire.command.close")

// FlushRequestCounter is to be incremented every time a flush request
// is made.
var FlushRequestCounter = telemetry.GetCounterOnce("pgwire.command.flush")
