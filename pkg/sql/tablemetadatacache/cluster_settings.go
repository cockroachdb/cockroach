// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tablemetadatacache

import (
	"errors"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

const defaultDataValidDuration = time.Minute * 20

var tableMetadataCacheAutoUpdatesEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"obs.tablemetadata.automatic_updates.enabled",
	"enables automatic updates of the table metadata cache system.table_metadata",
	false,
	settings.WithPublic)

var TableMetadataCacheValidDuration = settings.RegisterDurationSetting(
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
