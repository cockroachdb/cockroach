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

// UserDefinedSchemaTelemetryType represents a type of user defined schema
// related operation to record telemetry for.
type UserDefinedSchemaTelemetryType int

const (
	_ UserDefinedSchemaTelemetryType = iota
	// UserDefinedSchemaCreate represents a CREATE SCHEMA command.
	UserDefinedSchemaCreate
	// UserDefinedSchemaAlter represents an ALTER SCHEMA command.
	UserDefinedSchemaAlter
	// UserDefinedSchemaDrop represents a DROP SCHEMA command.
	UserDefinedSchemaDrop
	// UserDefinedSchemaReparentDatabase represents an ALTER DATABASE ... CONVERT TO SCHEMA command.
	UserDefinedSchemaReparentDatabase
	// UserDefinedSchemaUsedByObject tracks when an object is created in a user defined schema.
	UserDefinedSchemaUsedByObject
)

var userDefinedSchemaTelemetryMap = map[UserDefinedSchemaTelemetryType]string{
	UserDefinedSchemaCreate:           "create_schema",
	UserDefinedSchemaAlter:            "alter_schema",
	UserDefinedSchemaDrop:             "drop_schema",
	UserDefinedSchemaReparentDatabase: "reparent_database",
	UserDefinedSchemaUsedByObject:     "schema_used_by_object",
}

func (u UserDefinedSchemaTelemetryType) String() string {
	return userDefinedSchemaTelemetryMap[u]
}

var userDefinedSchemaTelemetryCounters map[UserDefinedSchemaTelemetryType]telemetry.Counter

func init() {
	userDefinedSchemaTelemetryCounters = make(map[UserDefinedSchemaTelemetryType]telemetry.Counter)
	for ty, s := range userDefinedSchemaTelemetryMap {
		userDefinedSchemaTelemetryCounters[ty] = telemetry.GetCounterOnce(fmt.Sprintf("sql.uds.%s", s))
	}
}

// IncrementUserDefinedSchemaCounter is used to increment the telemetry counter
// for a particular usage of user defined schemas.
func IncrementUserDefinedSchemaCounter(userDefinedSchemaType UserDefinedSchemaTelemetryType) {
	telemetry.Inc(userDefinedSchemaTelemetryCounters[userDefinedSchemaType])
}
