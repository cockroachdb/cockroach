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
	"log"
	"os"
	"os/exec"
)

func main() {
	log.SetFlags(0)
	log.SetPrefix("")

	// Disable ccache by setting CCACHE_DISABLE. We don't want it to write to files outside the bazel sandbox.
	if err := os.Setenv("CCACHE_DISABLE", "1"); err != nil {
		log.Fatal("Failed to set `CCACHE_DISABLE`")
	}
	if _, err := exec.LookPath("bazel"); err != nil {
		log.Printf("ERROR: bazel not found in $PATH")
		os.Exit(1)
	}

	dev := makeDevCmd()
	if err := dev.cli.Execute(); err != nil {
		log.Printf("ERROR: %v", err)
		os.Exit(1)
	}
}
