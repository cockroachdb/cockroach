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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

var (
	// flagEach controls how long we are going to run each compose test. Ensure bazel BUILD file
	// of compose tests has a longer timeout.
	flagEach       = flag.Duration("each", 10*time.Minute, "individual test timeout")
	flagTests      = flag.String("tests", ".", "tests within docker compose to run")
	flagArtifacts  = flag.String("artifacts", "", "artifact directory")
	flagCockroach  = flag.String("cockroach", "", "path to the cockroach executable")
	flagLibGeosDir = flag.String("libgeosdir", "", "path to the libgeos directory (only valid for bazel-driven test)")
	flagCompare    = flag.String("compare", "", "path to the compare test (only valid for bazel-driven test)")
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
	if os.Getenv("COCKROACH_RUN_COMPOSE") == "" {
		skip.IgnoreLint(t, "COCKROACH_RUN_COMPOSE not set")
	}
	var cockroachBin, libGeosDir, compareDir, dockerComposeYml string
	if bazel.BuiltWithBazel() {
		var err error
		dockerComposeYml, err = bazel.Runfile("pkg/compose/compare/docker-compose.yml")
		if err != nil {
			t.Fatal(err)
		}
		if *flagCockroach == "" {
			t.Fatal("-cockroach not set")
		}
		if *flagLibGeosDir == "" {
			t.Fatal("-libgeosdir not set")
		}
		if *flagCompare == "" {
			t.Fatal("-compare not set")
		}
		// These binaries are going to be mounted as volumes when we
		// start up docker-compose, but the files themselves will be
		// Bazel-built symlinks. We want to copy these files to a
		// different temporary location.
		composeBinsDir := t.TempDir()
		compareDir = composeBinsDir
		cockroachBin = filepath.Join(composeBinsDir, "cockroach")
		libGeosDir = filepath.Join(composeBinsDir, "lib")
		if err = os.MkdirAll(libGeosDir, 0755); err != nil {
			t.Fatal(err)
		}
		if err := copyBin(*flagCockroach, cockroachBin); err != nil {
			t.Fatal(err)
		}
		for _, geoLib := range []string{"libgeos.so", "libgeos_c.so"} {
			src := filepath.Join(*flagLibGeosDir, geoLib)
			dst := filepath.Join(libGeosDir, geoLib)
			if err := copyBin(src, dst); err != nil {
				t.Fatal(err)
			}
		}
		if err = copyBin(*flagCompare, filepath.Join(composeBinsDir, "compare.test")); err != nil {
			t.Fatal(err)
		}
		if *flagArtifacts == "" {
			*flagArtifacts = t.TempDir()
		}
	} else {
		if *flagCompare != "" {
			t.Fatal("should not set -compare unless test is driven with Bazel")
		}
		cockroachBin = *flagCockroach
		if cockroachBin == "" {
			cockroachBin = "../../../cockroach-linux-2.6.32-gnu-amd64"
		}
		libGeosDir = *flagLibGeosDir
		if libGeosDir == "" {
			libGeosDir = "../../../lib"
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
		fmt.Sprintf("LIBGEOS_DIR_PATH=%s", libGeosDir),
		fmt.Sprintf("COMPARE_DIR_PATH=%s", compareDir),
		fmt.Sprintf("ARTIFACTS=%s", *flagArtifacts),
		fmt.Sprintf("COCKROACH_DEV_LICENSE=%s", envutil.EnvOrDefaultString("COCKROACH_DEV_LICENSE", "")),
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		"COCKROACH_RUN_COMPOSE_COMPARE=true",
	}
	out, err := cmd.CombinedOutput()
	t.Log(string(out))
	if err != nil {
		t.Fatal(err)
	}
}
