// Copyright 2019 The Cockroach Authors.
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
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/testgen/workload/allccl"
)

func main() {
	allccl.GenerateTest(getPkgDir()+"/ccl/workloadccl/allccl/", prefix)
}

func getPkgDir() string {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalln(err)
	}

	return dir + "/pkg"
}

var prefix = `// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt
`
