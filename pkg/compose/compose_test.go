// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// "make test" would normally test this file, but it should only be tested
// during nightlies or when invoked by "make compose".

//go:build compose
// +build compose

// Package compose contains nightly tests that need docker-compose.
package compose

import (
	"flag"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
)

var (
	flagEach  = flag.Duration("each", 10*time.Minute, "individual test timeout")
	flagTests = flag.String("tests", ".", "tests within docker compose to run")
)

func TestComposeCompare(t *testing.T) {
	var dockerComposeYml string
	if bazel.BuiltWithBazel() {
		var err error
		dockerComposeYml, err = bazel.Runfile("pkg/compose/compare/docker-compose.yml")
		if err != nil {
			t.Fatal(err)
		}
	} else {
		dockerComposeYml = filepath.Join("compare", "docker-compose.yml")
	}
	cmd := exec.Command(
		"docker-compose",
		"-f", dockerComposeYml,
		"--no-ansi",
		"up",
		"--force-recreate",
		"--exit-code-from", "test",
	)
	cmd.Env = []string{
		fmt.Sprintf("EACH=%s", *flagEach),
		fmt.Sprintf("TESTS=%s", *flagTests),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Log(string(out))
		t.Fatal(err)
	}
}
