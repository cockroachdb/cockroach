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

	// CreateTempSequenceCounter is to be incremented every time a TEMP SEQUENCE
	// has been created.
	CreateTempSequenceCounter = telemetry.GetCounterOnce("sql.schema.create_temp_sequence")

	// CreateTempViewCounter is to be incremented every time a TEMP VIEW
	// has been created.
	CreateTempViewCounter = telemetry.GetCounterOnce("sql.schema.create_temp_view")
)

var (
	// HashShardedIndexCounter is to be incremented every time a hash
	// sharded index is created.
	HashShardedIndexCounter = telemetry.GetCounterOnce("sql.schema.hash_sharded_index")

	// InvertedIndexCounter is to be incremented every time an inverted index is
	// created. This includes single-column inverted indexes, geometry/geography
	// inverted indexes, multi-column inverted indexes, and partial inverted
	// indexes.
	InvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.inverted_index")

	// MultiColumnInvertedIndexCounter is to be incremented every time a
	// multi-column inverted index is created.
	MultiColumnInvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.multi_column_inverted_index")

	// GeographyInvertedIndexCounter is to be incremented every time a
	// geography inverted index is created. These are a subset of the
	// indexes counted in InvertedIndexCounter.
	GeographyInvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.geography_inverted_index")

	// GeometryInvertedIndexCounter is to be incremented every time a
	// geometry inverted index is created. These are a subset of the
	// indexes counted in InvertedIndexCounter.
	GeometryInvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.geometry_inverted_index")

	// PartialIndexCounter is to be incremented every time a partial index is
	// created. This includes both regular and inverted partial indexes.
	PartialIndexCounter = telemetry.GetCounterOnce("sql.schema.partial_index")

	// PartialInvertedIndexCounter is to be incremented every time a partial
	// inverted index is created.
	PartialInvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.partial_inverted_index")

	// PartitionedInvertedIndexCounter is to be incremented every time a
	// partitioned inverted index is created.
	PartitionedInvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.partitioned_inverted_index")

	// ExpressionIndexCounter is to be incremented every time an expression
	// index is created. This includes both regular and inverted expression
	// indexes.
	ExpressionIndexCounter = telemetry.GetCounterOnce("sql.schema.expression_index")
)

var (
	// TempObjectCleanerDeletionCounter is to be incremented every time a temporary schema
	// has been deleted by the temporary object cleaner.
	TempObjectCleanerDeletionCounter = telemetry.GetCounterOnce("sql.schema.temp_object_cleaner.num_cleaned")
)

// SchemaNewColumnTypeQualificationCounter is to be incremented every time
// a new qualification is used for a newly created column.
func SchemaNewColumnTypeQualificationCounter(qual string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.new_column.qualification." + qual)
}

// SchemaChangeCreateCounter is to be incremented every time a CREATE
// schema change was made.
func SchemaChangeCreateCounter(typ string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.create_" + typ)
}

// SchemaChangeDropCounter is to be incremented every time a DROP
// schema change was made.
func SchemaChangeDropCounter(typ string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.drop_" + typ)
}

// SchemaSetZoneConfigCounter is to be incremented every time a ZoneConfig
// argument is parsed.
func SchemaSetZoneConfigCounter(configName, keyChange string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("sql.schema.zone_config.%s.%s", configName, keyChange),
	)
}

// SchemaChangeAlterCounter behaves the same as SchemaChangeAlterCounterWithExtra
// but with no extra metadata.
func SchemaChangeAlterCounter(typ string) telemetry.Counter {
	return SchemaChangeAlterCounterWithExtra(typ, "")
}

// SchemaChangeAlterCounterWithExtra is to be incremented for ALTER schema changes.
// `typ` is for declaring which type was altered, e.g. TABLE, DATABASE.
// `extra` can be used for extra trailing useful metadata.
func SchemaChangeAlterCounterWithExtra(typ string, extra string) telemetry.Counter {
	if extra != "" {
		extra = "." + extra
	}
	return telemetry.GetCounter(fmt.Sprintf("sql.schema.alter_%s%s", typ, extra))
}

// SchemaSetAuditModeCounter is to be incremented every time an audit mode is set.
func SchemaSetAuditModeCounter(mode string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.set_audit_mode." + mode)
}

// SchemaJobControlCounter is to be incremented every time a job control action
// is taken.
func SchemaJobControlCounter(desiredStatus string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.job.control." + desiredStatus)
}

// SchemaChangeInExplicitTxnCounter is to be incremented every time a schema change
// is scheduled using an explicit transaction.
var SchemaChangeInExplicitTxnCounter = telemetry.GetCounterOnce("sql.schema.change_in_explicit_txn")

// SecondaryIndexColumnFamiliesCounter is a counter that is incremented every time
// a secondary index that is separated into different column families is created.
var SecondaryIndexColumnFamiliesCounter = telemetry.GetCounterOnce("sql.schema.secondary_index_column_families")

// CreateUnloggedTableCounter is a counter that is incremented every time an unlogged
// table is created.
var CreateUnloggedTableCounter = telemetry.GetCounterOnce("sql.schema.create_unlogged_table")

// SchemaRefreshMaterializedView is to be incremented every time a materialized
// view is refreshed.
var SchemaRefreshMaterializedView = telemetry.GetCounterOnce("sql.schema.refresh_materialized_view")

// SchemaChangeErrorCounter is to be incremented for different types
// of errors.
func SchemaChangeErrorCounter(typ string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("sql.schema_changer.errors.%s", typ))
}

// SetTableStorageParameter is to be incremented every time a table storage
// parameter has been SET (through CREATE TABLE or ALTER TABLE).
func SetTableStorageParameter(param string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.table_storage_parameter." + param + ".set")
}

// ResetTableStorageParameter is to be incremented every time a table storage
// parameter has been RESET.
func ResetTableStorageParameter(param string) telemetry.Counter {
	return telemetry.GetCounter("sql.schema.table_storage_parameter." + param + ".reset")
}
