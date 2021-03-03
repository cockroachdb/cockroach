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
	// CreateInterleavedTableCounter is to be incremented every time an
	// interleaved table is being created.
	CreateInterleavedTableCounter = telemetry.GetCounterOnce("sql.schema.create_interleaved_table")

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

	// InvertedIndexCounter is to be incremented every time an inverted
	// index is created.
	InvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.inverted_index")

	// GeographyInvertedIndexCounter is to be incremented every time a
	// geography inverted index is created. These are a subset of the
	// indexes counted in InvertedIndexCounter.
	GeographyInvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.geography_inverted_index")

	// GeometryInvertedIndexCounter is to be incremented every time a
	// geometry inverted index is created. These are a subset of the
	// indexes counted in InvertedIndexCounter.
	GeometryInvertedIndexCounter = telemetry.GetCounterOnce("sql.schema.geometry_inverted_index")

	// PartialIndexCounter is to be incremented every time a partial index is
	// created.
	PartialIndexCounter = telemetry.GetCounterOnce("sql.schema.partial_index")
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

// JobsForSchemaSuccess is a counter that incremented whenever a schema change
// job completes successfully.
var JobsForSchemaSuccess = telemetry.GetCounterOnce("sql.schema.job.schema_change_successful")

// JobsForSchemaFailed is a counter that incremented whenever a schema change
// job completes fails.
var JobsForSchemaFailed = telemetry.GetCounterOnce("sql.schema.job.schema_change_failed")

// JobsForSchemaCanceled is a counter that incremented whenever a schema change
// job gets canceled.
var JobsForSchemaCanceled = telemetry.GetCounterOnce("sql.schema.job.schema_change_canceled")

// JobsForSchemaGCSuccess is a counter that is incremented whenever a schema GC
// job completes successfully.
var JobsForSchemaGCSuccess = telemetry.GetCounterOnce("sql.schema.job.schemagc_change_successful")

// JobsForSchemaGCFailed is a counter that is incremented whenever a schema GC
// job fails.
var JobsForSchemaGCFailed = telemetry.GetCounterOnce("sql.schema.job.schemagc_change_failed")

// JobsForSchemaGCCanceled is a counter that is incremented whenever a schema GC
// job gets canceled.
var JobsForSchemaGCCanceled = telemetry.GetCounterOnce("sql.schema.job.schemagc_change_canceled")

// JobsForBackupSuccess is a counter that is incremented whenever a backup
// job completes successfully.
var JobsForBackupSuccess = telemetry.GetCounterOnce("sql.schema.job.backup_successful")

// JobsForBackupFailed is a counter that is incremented whenever a backup
// job fails.
var JobsForBackupFailed = telemetry.GetCounterOnce("sql.schema.job.backup_failed")

// JobsForBackupCanceled is a counter that is incremented whenever a backup
// job gets canceled.
var JobsForBackupCanceled = telemetry.GetCounterOnce("sql.schema.job.backup_canceled")

// JobsForRestoreSuccess is a counter that is incremented whenever a restore
// job completes successfully.
var JobsForRestoreSuccess = telemetry.GetCounterOnce("sql.schema.job.restore_successful")

// JobsForRestoreFailed is a counter that is incremented whenever a restore
// job fails.
var JobsForRestoreFailed = telemetry.GetCounterOnce("sql.schema.job.restore_failed")

// JobsForRestoreCanceled is a counter that is incremented whenever a restore
// job gets canceled.
var JobsForRestoreCanceled = telemetry.GetCounterOnce("sql.schema.job.restore_canceled")

// JobsForImportSuccess is a counter that is incremented whenever an import
// job completes successfully.
var JobsForImportSuccess = telemetry.GetCounterOnce("sql.schema.job.import_successful")

// JobsForImportFailed is a counter that is incremented whenever an import
// job fails.
var JobsForImportFailed = telemetry.GetCounterOnce("sql.schema.job.import_failed")

// JobsForImportCanceled is a counter that is incremented whenever an import
// job gets canceled.
var JobsForImportCanceled = telemetry.GetCounterOnce("sql.schema.job.import_canceled")

// JobsForChangeFeedSuccess is a counter that is incremented whenever a change feed
// job completes successfully.
var JobsForChangeFeedSuccess = telemetry.GetCounterOnce("sql.schema.job.changedfeed_successful")

// JobsForChangeFeedFailed is a counter that is incremented whenever a change feed
// job fails.
var JobsForChangeFeedFailed = telemetry.GetCounterOnce("sql.schema.job.changefeed_failed")

// JobsForChangeFeedCanceled is a counter that is incremented whenever a change feed
// job gets canceled.
var JobsForChangeFeedCanceled = telemetry.GetCounterOnce("sql.schema.job.changedfeed_canceled")

// JobsForCreateStatsSuccess is a counter that is incremented whenever a create stats
// job completes successfully.
var JobsForCreateStatsSuccess = telemetry.GetCounterOnce("sql.schema.job.createstats_successful")

// JobsForCreateStatsFailed is a counter that is incremented whenever a create stats
// job completes fails.
var JobsForCreateStatsFailed = telemetry.GetCounterOnce("sql.schema.job.createstats_failed")

// JobsForCreateStatsCanceled is a counter that is incremented whenever a create stats
// job gets canceled.
var JobsForCreateStatsCanceled = telemetry.GetCounterOnce("sql.schema.job.createstats_canceled")

// JobsForStreamIngestionSuccess is a counter that is incremented whenever a stream ingestion
// job completes successfully.
var JobsForStreamIngestionSuccess = telemetry.GetCounterOnce("sql.schema.job.streamingestion_successful")

// JobsForStreamIngestionFailed is a counter that is incremented whenever a stream ingestion
// job fails.
var JobsForStreamIngestionFailed = telemetry.GetCounterOnce("sql.schema.job.streamingestion_failed")

// JobsForStreamIngestionCanceled is a counter that is incremented whenever a stream ingestion
// job gets canceled.
var JobsForStreamIngestionCanceled = telemetry.GetCounterOnce("sql.schema.job.streamingetion_canceled")

// JobsForMigrationSuccess is a counter that is incremented whenever a stream ingestion
// job completes successfully.
var JobsForMigrationSuccess = telemetry.GetCounterOnce("sql.schema.job.migration_successful")

// JobsForMigrationFailed is a counter that is incremented whenever a stream ingestion
// job fails.
var JobsForMigrationFailed = telemetry.GetCounterOnce("sql.schema.job.migration_failed")

// JobsForMigrationCanceled is a counter that is incremented whenever a stream ingestion
// job gets canceled.
var JobsForMigrationCanceled = telemetry.GetCounterOnce("sql.schema.job.migration_canceled")
