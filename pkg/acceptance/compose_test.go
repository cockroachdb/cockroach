// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	maybeSkipTest(t)
	if bazel.BuiltWithBazel() {
		// Copy runfiles symlink content to a temporary directory to avoid broken symlinks in docker.
		tmpComposeDir := t.TempDir()
		err := copyRunfiles(composeDir, tmpComposeDir)
		if err != nil {
			t.Fatal(err)
		}
		path = filepath.Join(tmpComposeDir, path)
		// If running under Bazel, export 2 environment variables that will be interpolated in docker-compose.yml files.
		cockroachBinary, err := filepath.Abs(*cluster.CockroachBinary)
		if err != nil {
			t.Fatal(err)
		}
		err = os.Setenv("COCKROACH_BINARY", cockroachBinary)
		if err != nil {
			t.Fatal(err)
		}
		err = os.Setenv("CERTS_DIR", cluster.AbsCertsDir())
		if err != nil {
			t.Fatal(err)
		}
	} else {
		path = filepath.Join(composeDir, path)
	}
	uid := os.Getuid()
	err := os.Setenv("UID", strconv.Itoa(uid))
	if err != nil {
		t.Fatal(err)
	}
	gid := os.Getgid()
	err = os.Setenv("GID", strconv.Itoa(gid))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(
		"docker",
		"compose",
		// NB: Using --compatibility here in order to preserve compose V1 hostnames
		// (with underscores) instead of V2 hostnames (with -), because the
		// hostnames are hardcoded in the Kerberos keys.
		"--compatibility",
		"--ansi=never",
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
