// Copyright 2020 The Cockroach Authors.
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

// EnumTelemetryType represents a type of ENUM related operation to record
// telemetry for.
type EnumTelemetryType int

const (
	_ EnumTelemetryType = iota
	// EnumCreate represents a CREATE TYPE ... AS ENUM command.
	EnumCreate
	// EnumAlter represents an ALTER TYPE ... command.
	EnumAlter
	// EnumDrop represents a DROP TYPE command.
	EnumDrop
	// EnumInTable tracks when an enum type is used in a table.
	EnumInTable
)

var enumTelemetryMap = map[EnumTelemetryType]string{
	EnumCreate:  "create_enum",
	EnumAlter:   "alter_enum",
	EnumDrop:    "drop_enum",
	EnumInTable: "enum_used_in_table",
}

func (e EnumTelemetryType) String() string {
	return enumTelemetryMap[e]
}

var enumTelemetryCounters map[EnumTelemetryType]telemetry.Counter

func init() {
	enumTelemetryCounters = make(map[EnumTelemetryType]telemetry.Counter)
	for ty, s := range enumTelemetryMap {
		enumTelemetryCounters[ty] = telemetry.GetCounterOnce(fmt.Sprintf("sql.udts.%s", s))
	}
}

// IncrementEnumCounter is used to increment the telemetry counter for a particular
// usage of enums.
func IncrementEnumCounter(enumType EnumTelemetryType) {
	telemetry.Inc(enumTelemetryCounters[enumType])
}
