// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
