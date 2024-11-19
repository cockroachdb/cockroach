// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tablemetadatacache

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

const (
	defaultDataValidDuration = time.Minute * 20
	// defaultTableBatchSize is the number of tables to fetch in a
	// single batch from the system tables.
	defaultTableBatchSize = 20
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
	settings.WithValidateDuration(func(t time.Duration) error {
		// This prevents the update loop from running too frequently.
		if t < time.Minute {
			return errors.New("validity period can't be less than 1 minute")
		}
		return nil
	}),
	settings.WithPublic)

var updateJobBatchSizeSetting = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"obs.tablemetadata.update_job_batch_size",
	"the number of tables the update table metadata job will attempt to update "+
		"in a single batch",
	defaultTableBatchSize,
	settings.NonNegativeInt,
)
