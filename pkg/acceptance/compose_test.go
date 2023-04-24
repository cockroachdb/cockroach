// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package acceptance

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
)

const composeDir = "compose"

func TestCompose(t *testing.T) {
	t.Run("ComposeGSS", func(t *testing.T) {
		testCompose(t, filepath.Join("gss", "docker-compose.yml"), "psql")
	})
	t.Run("ComposeGSSPython", func(t *testing.T) {
		testCompose(t, filepath.Join("gss", "docker-compose-python.yml"), "python")
	})
	t.Run("ComposeGSSFlyway", func(t *testing.T) {
		testCompose(t, filepath.Join("flyway", "docker-compose.yml"), "flyway")
	})
}

func testCompose(t *testing.T, path string, exitCodeFrom string) {
	if bazel.BuiltWithBazel() {
		// Copy runfiles symlink content to a temporary directory to avoid broken symlinks in docker.
		tmpComposeDir := t.TempDir()
		err := copyRunfiles(composeDir, tmpComposeDir)
		if err != nil {
			t.Fatalf(err.Error())
		}
		path = filepath.Join(tmpComposeDir, path)
		// If running under Bazel, export 2 environment variables that will be interpolated in docker-compose.yml files.
		cockroachBinary, err := filepath.Abs(*cluster.CockroachBinary)
		if err != nil {
			t.Fatalf(err.Error())
		}
		err = os.Setenv("COCKROACH_BINARY", cockroachBinary)
		if err != nil {
			t.Fatalf(err.Error())
		}
		err = os.Setenv("CERTS_DIR", cluster.AbsCertsDir())
		if err != nil {
			t.Fatalf(err.Error())
		}
	} else {
		path = filepath.Join(composeDir, path)
	}
	uid := os.Getuid()
	err := os.Setenv("UID", strconv.Itoa(uid))
	if err != nil {
		t.Fatalf(err.Error())
	}
	gid := os.Getgid()
	err = os.Setenv("GID", strconv.Itoa(gid))
	if err != nil {
		t.Fatalf(err.Error())
	}
	cmd := exec.Command(
		"docker-compose",
		"--no-ansi",
		"-f", path,
		"up",
		"--force-recreate",
		"--build",
		"--exit-code-from", exitCodeFrom,
	)
	var buf bytes.Buffer
	if testing.Verbose() {
		cmd.Stdout = io.MultiWriter(&buf, os.Stdout)
		cmd.Stderr = io.MultiWriter(&buf, os.Stderr)
	} else {
		cmd.Stdout = &buf
		cmd.Stderr = &buf
	}
	if err := cmd.Run(); err != nil {
		t.Log(buf.String())
		t.Fatal(err)
	}
}
