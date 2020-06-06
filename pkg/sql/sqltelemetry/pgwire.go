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
var UncategorizedErrorCounter = telemetry.GetCounterOnce("othererror." + pgcode.Uncategorized.String())

// InterleavedPortalRequestCounter is to be incremented every time an open
// portal attempts to interleave work with another portal.
var InterleavedPortalRequestCounter = telemetry.GetCounterOnce("pgwire.#40195.interleaved_portal")

// PortalWithLimitRequestCounter is to be incremented every time a portal request is
// made.
var PortalWithLimitRequestCounter = telemetry.GetCounterOnce("pgwire.portal_with_limit_request")
