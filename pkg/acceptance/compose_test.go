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
	"testing"
)

func TestComposeGSS(t *testing.T) {
	testCompose(t, filepath.Join("compose", "gss", "docker-compose.yml"), "psql")
}

func TestComposeGSSPython(t *testing.T) {
	testCompose(t, filepath.Join("compose", "gss", "docker-compose-python.yml"), "python")
}

func TestComposeFlyway(t *testing.T) {
	testCompose(t, filepath.Join("compose", "flyway", "docker-compose.yml"), "flyway")
}

func testCompose(t *testing.T, path string, exitCodeFrom string) {
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
