// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

const (
	defaultDataValidDuration = time.Minute * 20
	// defaultTableBatchSize is the number of tables to fetch in a
	// single batch from the system tables.
	defaultTableBatchSize = 20
	// defaultPruneInterval is the default interval for the prune job.
	defaultPruneInterval = time.Hour
)

var AutomaticCacheUpdatesEnabledSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"obs.tablemetadata.automatic_updates.enabled",
	"enables automatic updates of the table metadata cache system.table_metadata",
	false,
	settings.WithPublic)

var DataValidDurationSetting = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"obs.tablemetadata.data_valid_duration",
	"the duration for which the data in system.table_metadata is considered valid",
	defaultDataValidDuration,
	// This prevents the update loop from running too frequently.
	settings.DurationWithMinimum(time.Minute),
	settings.WithPublic)

var updateJobBatchSizeSetting = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"obs.tablemetadata.update_job_batch_size",
	"the number of tables the update table metadata job will attempt to update "+
		"in a single batch",
	defaultTableBatchSize,
	settings.NonNegativeInt,
)

// PruneIntervalSetting controls the interval at which the prune job runs.
var PruneIntervalSetting = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"obs.tablemetadata.prune_interval",
	"the interval at which the table metadata cache prune job runs",
	defaultPruneInterval,
	// Prevent the prune job from running too frequently.
	settings.DurationWithMinimum(time.Minute),
	settings.WithPublic)
