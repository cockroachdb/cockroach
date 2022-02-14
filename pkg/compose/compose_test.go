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
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
)

var (
	flagEach      = flag.Duration("each", 10*time.Minute, "individual test timeout")
	flagTests     = flag.String("tests", ".", "tests within docker compose to run")
	flagArtifacts = flag.String("artifacts", "", "artifact directory")
	flagCockroach = flag.String("cockroach", "", "path to the cockroach executable")
	flagCompare   = flag.String("compare", "", "path to the compare test (only valid for bazel-driven test)")
)

func copyBin(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	_, err = io.Copy(out, in)
	return err
}

func TestComposeCompare(t *testing.T) {
	var cockroachBin, compareDir, dockerComposeYml string
	if bazel.BuiltWithBazel() {
		var err error
		dockerComposeYml, err = bazel.Runfile("pkg/compose/compare/docker-compose.yml")
		if err != nil {
			t.Fatal(err)
		}
		if *flagCockroach == "" {
			t.Fatal("-cockroach not set")
		}
		if *flagCompare == "" {
			t.Fatal("-compare not set")
		}
		// These binaries are going to be mounted as volumes when we
		// start up docker-compose, but the files themselves will be
		// Bazel-built symlinks. We want to copy these files to a
		// different temporary location.
		composeBinsDir, err := ioutil.TempDir("", "compose-bins")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.RemoveAll(composeBinsDir) }()
		compareDir = composeBinsDir
		cockroachBin = filepath.Join(composeBinsDir, "cockroach")
		err = copyBin(*flagCockroach, cockroachBin)
		if err != nil {
			t.Fatal(err)
		}
		err = copyBin(*flagCompare, filepath.Join(composeBinsDir, "compare.test"))
		if err != nil {
			t.Fatal(err)
		}
		if *flagArtifacts == "" {
			*flagArtifacts, err = ioutil.TempDir("", "compose")
			if err != nil {
				t.Fatal(err)
			}
		}
	} else {
		if *flagCompare != "" {
			t.Fatal("should not set -compare unless test is driven with Bazel")
		}
		cockroachBin = *flagCockroach
		if cockroachBin == "" {
			cockroachBin = "../../../cockroach-linux-2.6.32-gnu-amd64"
		}
		compareDir = "./compare"
		dockerComposeYml = filepath.Join("compare", "docker-compose.yml")
		if *flagArtifacts == "" {
			*flagArtifacts = compareDir
		}
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
		fmt.Sprintf("COCKROACH_PATH=%s", cockroachBin),
		fmt.Sprintf("COMPARE_DIR_PATH=%s", compareDir),
		fmt.Sprintf("ARTIFACTS=%s", *flagArtifacts),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Log(string(out))
		t.Fatal(err)
	}
}
