// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

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
