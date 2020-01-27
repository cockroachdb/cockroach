// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package compose contains nightly tests that need docker-compose.
package compose

import (
	"flag"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

var (
	flagEach  = flag.Duration("each", 10*time.Minute, "individual test timeout")
	flagTests = flag.String("tests", ".", "tests within docker compose to run")
)

func TestComposeCompare(t *testing.T) {
	const file = "docker-compose"
	if _, err := exec.LookPath(file); err != nil {
		t.Skip(err)
	}
	cmd := exec.Command(
		file,
		"-f", filepath.Join("compare", "docker-compose.yml"),
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
