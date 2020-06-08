// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// compile-builds attempts to compile all CRDB builds we support.

package main

import (
	"go/build"
	"log"

	"github.com/cockroachdb/cockroach/pkg/release"
)

func main() {
	pkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}

	for _, target := range release.SupportedTargets {
		if err := release.MakeRelease(
			target,
			pkg.Dir,
		); err != nil {
			log.Fatal(err)
		}
	}
}
