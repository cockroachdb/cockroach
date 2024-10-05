// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package allccl

// We import each of the workloads below, so a single import of this package
// enables registration of all workloads.

import (
	// workloads
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/roachmartccl"
	_ "github.com/cockroachdb/cockroach/pkg/workload/bank"
	_ "github.com/cockroachdb/cockroach/pkg/workload/bulkingest"
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
	_ "github.com/cockroachdb/cockroach/pkg/workload/schemachange"
	_ "github.com/cockroachdb/cockroach/pkg/workload/sqlsmith"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpccchecks"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpcds"
	_ "github.com/cockroachdb/cockroach/pkg/workload/tpch"
	_ "github.com/cockroachdb/cockroach/pkg/workload/ttlbench"
	_ "github.com/cockroachdb/cockroach/pkg/workload/ttllogger"
	_ "github.com/cockroachdb/cockroach/pkg/workload/ycsb"
)
