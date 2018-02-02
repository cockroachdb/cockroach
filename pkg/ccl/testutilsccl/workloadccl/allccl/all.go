// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package allccl

// We import each of the workloads below, so a single import of this package
// enables registration of all workloads.

import (
	// workloads
	_ "github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl/workloadccl/roachmartccl"
	_ "github.com/cockroachdb/cockroach/pkg/testutils/workload/bank"
	_ "github.com/cockroachdb/cockroach/pkg/testutils/workload/kv"
	_ "github.com/cockroachdb/cockroach/pkg/testutils/workload/tpcc"
)
