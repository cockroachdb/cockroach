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

import "github.com/cockroachdb/cockroach/pkg/settings/cluster"

// AutoStatsCollectionEnabled indicates if automatic statistics collection is
// explicitly enabled or disabled.
func (cs *ClusterSettingsForTable) AutoStatsCollectionEnabled() cluster.BoolSetting {
	if cs.SqlStatsAutomaticCollectionEnabled == nil {
		return cluster.NotSet
	}
	if *cs.SqlStatsAutomaticCollectionEnabled {
		return cluster.True
	}
	return cluster.False
}

// AutoStatsMinStaleRows indicates the setting of
// sql.stats.automatic_collection.min_stale_rows in ClusterSettingsForTable.
// If ok is true, then the minStaleRows value is valid, otherwise this has not
// been set.
func (cs *ClusterSettingsForTable) AutoStatsMinStaleRows() (minStaleRows int64, ok bool) {
	if cs.SqlStatsAutomaticCollectionMinStaleRows == nil {
		return 0, false
	}
	return *cs.SqlStatsAutomaticCollectionMinStaleRows, true
}

// AutoStatsFractionStaleRows indicates the setting of
// sql.stats.automatic_collection.fraction_stale_rows in
// ClusterSettingsForTable. If ok is true, then the fractionStaleRows value is
// valid, otherwise this has not been set.
func (cs *ClusterSettingsForTable) AutoStatsFractionStaleRows() (
	fractionStaleRows float64,
	ok bool,
) {
	if cs.SqlStatsAutomaticCollectionFractionStaleRows == nil {
		return 0, false
	}
	return *cs.SqlStatsAutomaticCollectionFractionStaleRows, true
}

// NoAutoStatsSettingsOverrides is true if no auto stats related cluster
// settings are present in these ClusterSettingsForTable.
func (cs *ClusterSettingsForTable) NoAutoStatsSettingsOverrides() bool {
	if cs == nil {
		return true
	}
	if cs.SqlStatsAutomaticCollectionEnabled != nil ||
		cs.SqlStatsAutomaticCollectionMinStaleRows != nil ||
		cs.SqlStatsAutomaticCollectionFractionStaleRows != nil {
		return false
	}
	return true
}

// AutoStatsSettingsEqual returns true if all auto stats related
// ClusterSettingsForTable in this structure match those in `other`. An unset
// value must also be unset in the other's settings to be equal.
func (cs *ClusterSettingsForTable) AutoStatsSettingsEqual(other *ClusterSettingsForTable) bool {
	otherHasNoAutoStatsSettings := other.NoAutoStatsSettingsOverrides()
	if cs.NoAutoStatsSettingsOverrides() {
		return otherHasNoAutoStatsSettings
	}
	if otherHasNoAutoStatsSettings {
		return false
	}
	if cs.AutoStatsCollectionEnabled() != other.AutoStatsCollectionEnabled() {
		return false
	}
	minStaleRows := cs.SqlStatsAutomaticCollectionMinStaleRows
	otherMinStaleRows := other.SqlStatsAutomaticCollectionMinStaleRows
	if minStaleRows == nil {
		if otherMinStaleRows != nil {
			return false
		}
	}
	if otherMinStaleRows == nil {
		if minStaleRows != nil {
			return false
		}
	}
	if *minStaleRows != *otherMinStaleRows {
		return false
	}

	fractionStaleRows := cs.SqlStatsAutomaticCollectionFractionStaleRows
	otherFractionStaleRows := other.SqlStatsAutomaticCollectionFractionStaleRows
	if fractionStaleRows == nil {
		if otherFractionStaleRows != nil {
			return false
		}
	}
	if otherFractionStaleRows == nil {
		if fractionStaleRows != nil {
			return false
		}
	}
	if *fractionStaleRows != *otherFractionStaleRows {
		return false
	}
	return true
}
