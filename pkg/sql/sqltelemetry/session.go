// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// CockroachShellCounter is to be incremented every time a
// client uses the Cockroach SQL shell to connect to CockroachDB
var CockroachShellCounter = telemetry.GetCounterOnce("sql.connection.cockroach_cli")

// UnimplementedSessionVarValueCounter is to be incremented every time
// a client attempts to set a compatitibility session var to an
// unsupported value.
func UnimplementedSessionVarValueCounter(varName, val string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("unimplemented.sql.session_var.%s.%s", varName, val))
}

// DummySessionVarValueCounter is to be incremented every time
// a client attempts to set a compatitibility session var to a
// dummy value.
func DummySessionVarValueCounter(varName string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("sql.session_var.dummy.%s", varName))
}
