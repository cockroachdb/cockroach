// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
