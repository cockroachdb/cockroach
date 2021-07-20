// Copyright 2021 The Cockroach Authors.
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

var (
	// CreateMultiRegionDatabaseCounter is to be incremented when a multi-region
	// database is created.
	CreateMultiRegionDatabaseCounter = telemetry.GetCounterOnce(
		"sql.multiregion.create_database",
	)
	// SetInitialPrimaryRegionCounter is to be incremented when
	// a multi-region database is created using ALTER DATABASE ... PRIMARY KEY.
	SetInitialPrimaryRegionCounter = telemetry.GetCounterOnce(
		"sql.multiregion.alter_database.set_primary_region.initial_multiregion",
	)
	// SwitchPrimaryRegionCounter is to be incremented when
	// a multi-region database has its primary region changed.
	SwitchPrimaryRegionCounter = telemetry.GetCounterOnce(
		"sql.multiregion.alter_database.set_primary_region.switch_primary_region",
	)

	// AlterDatabaseAddRegionCounter is to be incremented when a region is
	// added to a database.
	AlterDatabaseAddRegionCounter = telemetry.GetCounterOnce(
		"sql.multiregion.add_region",
	)

	// AlterDatabaseDropRegionCounter is to be incremented when a non-primary
	// region is dropped from a database.
	AlterDatabaseDropRegionCounter = telemetry.GetCounterOnce(
		"sql.multiregion.drop_region",
	)

	// AlterDatabaseDropPrimaryRegionCounter is to be incremented when a primary
	// region is dropped from a database.
	AlterDatabaseDropPrimaryRegionCounter = telemetry.GetCounterOnce(
		"sql.multiregion.drop_primary_region",
	)

	// ImportIntoMultiRegionDatabaseCounter is to be incremented when an import
	// statement is run against a multi-region database.
	ImportIntoMultiRegionDatabaseCounter = telemetry.GetCounterOnce(
		"sql.multiregion.import",
	)

	// OverrideMultiRegionZoneConfigurationUser is to be incremented when a
	// multi-region zone configuration is overridden by the user.
	OverrideMultiRegionZoneConfigurationUser = telemetry.GetCounterOnce(
		"sql.multiregion.zone_configuration.override.user",
	)

	// OverrideMultiRegionDatabaseZoneConfigurationSystem is to be incremented
	// when a multi-region database zone configuration is overridden by the
	// system.
	OverrideMultiRegionDatabaseZoneConfigurationSystem = telemetry.GetCounterOnce(
		"sql.multiregion.zone_configuration.override.system.database",
	)

	// OverrideMultiRegionTableZoneConfigurationSystem is to be incremented when
	// a multi-region table/index/partition zone configuration is overridden by
	// the system.
	OverrideMultiRegionTableZoneConfigurationSystem = telemetry.GetCounterOnce(
		"sql.multiregion.zone_configuration.override.system.table",
	)
)

// CreateDatabaseSurvivalGoalCounter is to be incremented when the survival goal
// on a multi-region database is being set.
func CreateDatabaseSurvivalGoalCounter(goal string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("sql.multiregion.create_database.survival_goal.%s", goal))
}

// AlterDatabaseSurvivalGoalCounter is to be incremented when the survival goal
// on a multi-region database is being altered.
func AlterDatabaseSurvivalGoalCounter(goal string) telemetry.Counter {
	return telemetry.GetCounter(fmt.Sprintf("sql.multiregion.alter_database.survival_goal.%s", goal))
}

// CreateTableLocalityCounter is to be incremented every time a locality
// is set on a table.
func CreateTableLocalityCounter(locality string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("sql.multiregion.create_table.locality.%s", locality),
	)
}

// AlterTableLocalityCounter is to be incremented every time a locality
// is changed on a table.
func AlterTableLocalityCounter(from, to string) telemetry.Counter {
	return telemetry.GetCounter(
		fmt.Sprintf("sql.multiregion.alter_table.locality.from.%s.to.%s", from, to),
	)
}
