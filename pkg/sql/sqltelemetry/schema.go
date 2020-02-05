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
)

// SerialColumnNormalizationCounter is to be incremented every time
// a SERIAL type is processed in a column definition.
// It includes the normalization type, so we can
// estimate usage of the various normalization strategies.
func SerialColumnNormalizationCounter(inputType, normType string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("sql.schema.serial.%s.%s", normType, inputType))
}

// SchemaNewTypeCounter is to be incremented every time a new data type
// is used in a schema, i.e. by CREATE TABLE or ALTER TABLE ADD COLUMN.
func SchemaNewTypeCounter(t string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.new_column_type." + t)
}

var (
	// CreateTempTableCounter is to be incremented every time a TEMP TABLE
	// has been created.
	CreateTempTableCounter = telemetry.GetCounterOnce("sql.schema.create_temp_table")

	// CreateTempViewCounter is to be incremented every time a TEMP VIEW
	// has been created.
	CreateTempViewCounter = telemetry.GetCounterOnce("sql.schema.create_temp_view")
)

// SecondaryIndexColumnFamiliesCounter is a counter that is incremented every time
// a secondary index that is separated into different column families is created.
var SecondaryIndexColumnFamiliesCounter = telemetry.GetCounterOnce("sql.schema.secondary_index_column_families")

// AlterPrimaryKeyCounter is a counter that is incremented every time the
// ALTER PRIMARY KEY command is used.
var AlterPrimaryKeyCounter = telemetry.GetCounterOnce("sql.schema.alter_primary_key")
