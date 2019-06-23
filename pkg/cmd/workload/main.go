// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
