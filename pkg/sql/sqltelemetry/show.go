// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltelemetry

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
)

// ShowTelemetryType is an enum used to represent the different show commands
// that we are recording telemetry for.
type ShowTelemetryType int

const (
	_ ShowTelemetryType = iota
	// Ranges represents the SHOW RANGES command.
	Ranges
	// Regions represents the SHOW REGIONS command.
	Regions
	// RegionsFromCluster represents the SHOW REGIONS FROM CLUSTER command.
	RegionsFromCluster
	// RegionsFromAllDatabases represents the SHOW REGIONS FROM ALL DATABASES command.
	RegionsFromAllDatabases
	// RegionsFromDatabase represents the SHOW REGIONS FROM DATABASE command.
	RegionsFromDatabase
	// SurvivalGoal represents the SHOW SURVIVAL GOAL command.
	SurvivalGoal
	// Partitions represents the SHOW PARTITIONS command.
	Partitions
	// Locality represents the SHOW LOCALITY command.
	Locality
	// Create represents the SHOW CREATE command.
	Create
	// CreateSchedule represents the SHOW CREATE SCHEDULE command.
	CreateSchedule
	// RangeForRow represents the SHOW RANGE FOR ROW command.
	RangeForRow
	// Queries represents the SHOW QUERIES command.
	Queries
	// Indexes represents the SHOW INDEXES command.
	Indexes
	// Constraints represents the SHOW CONSTRAINTS command.
	Constraints
	// Jobs represents the SHOW JOBS command.
	Jobs
	// Roles represents the SHOW ROLES command.
	Roles
	// Schedules represents the SHOW SCHEDULE command.
	Schedules
	// FullTableScans represents the SHOW FULL TABLE SCANS command.
	FullTableScans
	// SuperRegions represents the SHOW SUPER REGIONS command.
	SuperRegions
	// CreateExternalConnection represents the SHOW CREATE EXTERNAL CONNECTION command.
	CreateExternalConnection
	// ExternalConnection represents the SHOW EXTERNAL CONNECTION command.
	ExternalConnection
	// LogicalReplicationJobs represents the SHOW LOGICAL REPLICATION JOBS command.
	LogicalReplicationJobs
)

var showTelemetryNameMap = map[ShowTelemetryType]string{
	Ranges:                   "ranges",
	Partitions:               "partitions",
	Locality:                 "locality",
	Create:                   "create",
	CreateSchedule:           "create_schedule",
	RangeForRow:              "rangeforrow",
	Regions:                  "regions",
	RegionsFromCluster:       "regions_from_cluster",
	RegionsFromDatabase:      "regions_from_database",
	RegionsFromAllDatabases:  "regions_from_all_databases",
	SurvivalGoal:             "survival_goal",
	Queries:                  "queries",
	Indexes:                  "indexes",
	Constraints:              "constraints",
	Jobs:                     "jobs",
	Roles:                    "roles",
	Schedules:                "schedules",
	FullTableScans:           "full_table_scans",
	SuperRegions:             "super_regions",
	CreateExternalConnection: "create_external_connection",
	ExternalConnection:       "external_connection",
	LogicalReplicationJobs:   "logical_replication_jobs",
}

func (s ShowTelemetryType) String() string {
	return showTelemetryNameMap[s]
}

var showTelemetryCounters map[ShowTelemetryType]telemetry.Counter

func init() {
	showTelemetryCounters = make(map[ShowTelemetryType]telemetry.Counter)
	for ty, s := range showTelemetryNameMap {
		showTelemetryCounters[ty] = telemetry.GetCounterOnce(fmt.Sprintf("sql.show.%s", s))
	}
}

// IncrementShowCounter is used to increment the telemetry counter for a particular show command.
func IncrementShowCounter(showType ShowTelemetryType) {
	telemetry.Inc(showTelemetryCounters[showType])
}
