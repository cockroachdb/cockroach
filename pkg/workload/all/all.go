// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package all aggregates blank imports of every workload generator so a
// single import enables registration of all workloads.
package all

import (
	// workloads
	_ "github.com/cockroachdb/cockroach/pkg/workload/bank"
	_ "github.com/cockroachdb/cockroach/pkg/workload/bulkingest"
	_ "github.com/cockroachdb/cockroach/pkg/workload/conflict"
	_ "github.com/cockroachdb/cockroach/pkg/workload/connectionlatency"
	_ "github.com/cockroachdb/cockroach/pkg/workload/debug"
	_ "github.com/cockroachdb/cockroach/pkg/workload/examples"
	_ "github.com/cockroachdb/cockroach/pkg/workload/geospatial"
	_ "github.com/cockroachdb/cockroach/pkg/workload/indexes"
	_ "github.com/cockroachdb/cockroach/pkg/workload/insights"
	_ "github.com/cockroachdb/cockroach/pkg/workload/jsonload"
	_ "github.com/cockroachdb/cockroach/pkg/workload/kv"
	_ "github.com/cockroachdb/cockroach/pkg/workload/ledger"
	_ "github.com/cockroachdb/cockroach/pkg/workload/movr"
	_ "github.com/cockroachdb/cockroach/pkg/workload/querybench"
	_ "github.com/cockroachdb/cockroach/pkg/workload/querylog"
	_ "github.com/cockroachdb/cockroach/pkg/workload/queue"
	_ "github.com/cockroachdb/cockroach/pkg/workload/rand"
	_ "github.com/cockroachdb/cockroach/pkg/workload/roachmart"
	_ "github.com/cockroachdb/cockroach/pkg/workload/schemachange"
	_ "github.com/cockroachdb/cockroach/pkg/workload/sqlsmith"
	_ "github.com/cockroachdb/cockroach/pkg/workload/sqlstats"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpccchecks"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcds"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpch"
	_ "github.com/cockroachdb/cockroach/pkg/workload/ttlbench"
	_ "github.com/cockroachdb/cockroach/pkg/workload/ttllogger"
	_ "github.com/cockroachdb/cockroach/pkg/workload/uniqueindex"
	_ "github.com/cockroachdb/cockroach/pkg/workload/vecann"
	_ "github.com/cockroachdb/cockroach/pkg/workload/workload_generator"
	_ "github.com/cockroachdb/cockroach/pkg/workload/ycsb"
)
