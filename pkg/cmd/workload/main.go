// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"

	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/allccl" // init hooks
	_ "github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/cliccl" // init hooks
	workloadcli "github.com/cockroachdb/cockroach/pkg/workload/cli"
)

func main() {
	if err := workloadcli.WorkloadCmd(false /* userFacing */).Execute(); err != nil {
		// Cobra has already printed the error message.
		os.Exit(1)
	}
}
