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

// SchemaChangeCreate is to be incremented every time a CREATE
// schema change was made.
func SchemaChangeCreate(typ string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.create_" + typ)
}

// SchemaChangeDrop is to be incremented every time a DROP
// schema change was made.
func SchemaChangeDrop(typ string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.drop_" + typ)
}

// SchemaSetZoneConfig is to be incremented every time a ZoneConfig
// argument is parsed.
func SchemaSetZoneConfig(configName, keyChange string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("sql.schema.zone_config.%s.%s", configName, keyChange),
	)
}

// SchemaChangeAlter behaves the same as SchemaChangeAlterWithExtra
// but with no extra metadata.
func SchemaChangeAlter(typ string) telemetry.Counter {
	return SchemaChangeAlterWithExtra(typ, "")
}

// SchemaChangeAlterWithExtra is to be incremented for ALTER schema changes.
// `typ` is for declaring which type was altered, e.g. TABLE, DATABASE.
// `extra` can be used for extra trailing useful metadata.
func SchemaChangeAlterWithExtra(typ string, extra string) telemetry.Counter {
	if extra != "" {
		extra = "." + extra
	}
	return telemetry.GetCounter(fmt.Sprintf("sql.schema.alter_%s%s", typ, extra))
}

// SecondaryIndexColumnFamiliesCounter is a counter that is incremented every time
// a secondary index that is separated into different column families is created.
var SecondaryIndexColumnFamiliesCounter = telemetry.GetCounterOnce("sql.schema.secondary_index_column_families")
