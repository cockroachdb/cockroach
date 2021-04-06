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
)

var showTelemetryNameMap = map[ShowTelemetryType]string{
	Ranges:                  "ranges",
	Partitions:              "partitions",
	Locality:                "locality",
	Create:                  "create",
	RangeForRow:             "rangeforrow",
	Regions:                 "regions",
	RegionsFromCluster:      "regions_from_cluster",
	RegionsFromDatabase:     "regions_from_database",
	RegionsFromAllDatabases: "regions_from_all_databases",
	SurvivalGoal:            "survival_goal",
	Queries:                 "queries",
	Indexes:                 "indexes",
	Constraints:             "constraints",
	Jobs:                    "jobs",
	Roles:                   "roles",
	Schedules:               "schedules",
	FullTableScans:          "full_table_scans",
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
