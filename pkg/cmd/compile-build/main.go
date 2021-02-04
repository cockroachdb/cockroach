// Copyright 2020 The Cockroach Authors.
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
	"flag"
	"go/build"
	"log"

	"github.com/cockroachdb/cockroach/pkg/release"
)

func main() {
	pkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}

	var compileAll = flag.Bool("all", false, "compile all supported builds (darwin, linux, windows)")
	var buildType = flag.String("buildtype", "release", "compile with a different build type. Default: 'release'. Options: 'development', 'release'")
	flag.Parse()

	// We compile just the first supported target unless we explicitly told to
	// cross compile.
	targets := release.SupportedTargets[:1]
	if *compileAll {
		targets = release.SupportedTargets
	}
	opts := []release.MakeReleaseOption{
		release.WithMakeReleaseOptionEnv("MKRELEASE_BUILDTYPE=" + *buildType),
	}

	for _, target := range targets {
		if err := release.MakeRelease(
			target,
			pkg.Dir,
			opts...,
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := release.MakeWorkload(pkg.Dir); err != nil {
		log.Fatal(err)
	}
}
