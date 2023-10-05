// Copyright 2023 The Cockroach Authors.
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
)

// IncrementPlpgsqlStmtCounter is to be incremented every time a new plpgsql stmt is
// used for a newly created function.
func IncrementPlpgsqlStmtCounter(stmtTag string) {
	telemetry.Inc(telemetry.GetCounter("sql.plpgsql." + stmtTag))
}

const builtinFunctionTelemetryPrefix = "sql.builtins"

// BuiltinFunctionTelemetryType is an enum for the type of builtin function
// that we are collecting telemetry for.
type BuiltinFunctionTelemetryType int

const (
	_ BuiltinFunctionTelemetryType = iota
	// CryptoBuiltinFunction is used for cryptographic builtin functions.
	CryptoBuiltinFunction
)

var builtinFunctionTelemetryNameMap = map[BuiltinFunctionTelemetryType]string{
	CryptoBuiltinFunction: "crypto",
}

var builtinFunctionTelemetryCounters map[BuiltinFunctionTelemetryType]map[string]telemetry.Counter

func init() {
	builtinFunctionTelemetryCounters = make(map[BuiltinFunctionTelemetryType]map[string]telemetry.Counter)
	for typ := range builtinFunctionTelemetryNameMap {
		builtinFunctionTelemetryCounters[typ] = make(map[string]telemetry.Counter)
	}
}

// IncBuiltinFunctionCounter increments counter for the specified builtin.
func IncBuiltinFunctionCounter(builtinType BuiltinFunctionTelemetryType, builtinName string) {
	builtinTypeName := builtinFunctionTelemetryNameMap[builtinType]
	builtinTypeCounters := builtinFunctionTelemetryCounters[builtinType]
	if builtinTypeName == "" || builtinTypeCounters == nil {
		// If the builtin type is missing from either map then it must be invalid.
		return
	}

	counter, ok := builtinTypeCounters[builtinName]
	if !ok {
		counter = telemetry.GetCounter(fmt.Sprintf("%s.%s.%s",
			builtinFunctionTelemetryPrefix, builtinTypeName, builtinName))
		builtinTypeCounters[builtinName] = counter
	}

	telemetry.Inc(counter)
}
