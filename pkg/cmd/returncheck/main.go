// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/returncheck"
	"github.com/kisielk/gotool"
)

func main() {
	targetPkg := "github.com/cockroachdb/cockroach/pkg/roachpb"
	targetTypeName := "Error"
	if err := returncheck.Run(gotool.ImportPaths(os.Args[1:]), targetPkg, targetTypeName); err != nil {
		os.Exit(1)
	}
}
