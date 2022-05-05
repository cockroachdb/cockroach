// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catpb

// AutoStatsCollectionStatus represents whether the auto stats collections
// enabled table setting is enabled, disabled, or not set.
type AutoStatsCollectionStatus int

// The values for AutoStatsCollectionStatus.
const (
	AutoStatsCollectionNotSet AutoStatsCollectionStatus = iota
	AutoStatsCollectionEnabled
	AutoStatsCollectionDisabled
)

const (
	// AutoStatsEnabledSettingName is the name of the automatic stats collection
	// enabled cluster setting.
	AutoStatsEnabledSettingName = "sql.stats.automatic_collection.enabled"

	// AutoStatsEnabledTableSettingName is the name of the automatic stats
	// collection enabled table setting.
	AutoStatsEnabledTableSettingName = "sql_stats_automatic_collection_enabled"

	// AutoStatsMinStaleSettingName is the name of the automatic stats collection
	// min stale rows cluster setting.
	AutoStatsMinStaleSettingName = "sql.stats.automatic_collection.min_stale_rows"

	// UseStatsOnSystemTables is the name of the use statistics on system tables
	// cluster setting.
	UseStatsOnSystemTables = "sql.stats.system_tables.enabled"

	// AutoStatsOnSystemTables is the name of the autostats on system tables
	// cluster setting.
	AutoStatsOnSystemTables = "sql.stats.system_tables_autostats.enabled"

	// AutoStatsMinStaleTableSettingName is the name of the automatic stats collection
	// min stale rows table setting.
	AutoStatsMinStaleTableSettingName = "sql_stats_automatic_collection_min_stale_rows"

	// AutoStatsFractionStaleSettingName is the name of the automatic stats
	// collection fraction stale rows cluster setting.
	AutoStatsFractionStaleSettingName = "sql.stats.automatic_collection.fraction_stale_rows"

	// AutoStatsFractionStaleTableSettingName is the name of the automatic stats
	// collection fraction stale rows table setting.
	AutoStatsFractionStaleTableSettingName = "sql_stats_automatic_collection_fraction_stale_rows"
)

// AutoStatsCollectionEnabled indicates if automatic statistics collection is
// explicitly enabled or disabled.
func (as *AutoStatsSettings) AutoStatsCollectionEnabled() AutoStatsCollectionStatus {
	if as.Enabled == nil {
		return AutoStatsCollectionNotSet
	}
	if *as.Enabled {
		return AutoStatsCollectionEnabled
	}
	return AutoStatsCollectionDisabled
}

// AutoStatsMinStaleRows indicates the setting of
// sql_stats_automatic_collection_min_stale_rows in AutoStatsSettings. If ok is
// true, then the minStaleRows value is valid, otherwise this has not been set.
func (as *AutoStatsSettings) AutoStatsMinStaleRows() (minStaleRows int64, ok bool) {
	if as.MinStaleRows == nil {
		return 0, false
	}
	return *as.MinStaleRows, true
}

// AutoStatsFractionStaleRows indicates the setting of
// sql_stats_automatic_collection_fraction_stale_rows in AutoStatsSettings. If
// ok is true, then the fractionStaleRows value is valid, otherwise this has not
// been set.
func (as *AutoStatsSettings) AutoStatsFractionStaleRows() (fractionStaleRows float64, ok bool) {
	if as.FractionStaleRows == nil {
		return 0, false
	}
	return *as.FractionStaleRows, true
}

// NoAutoStatsSettingsOverrides is true if no auto stats related table
// settings are present in these AutoStatsSettings.
func (as *AutoStatsSettings) NoAutoStatsSettingsOverrides() bool {
	if as.Enabled != nil ||
		as.MinStaleRows != nil ||
		as.FractionStaleRows != nil {
		return false
	}
	return true
}
