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
	"os/exec"
	"path/filepath"
	"testing"
)

func TestComposeGSS(t *testing.T) {
	out, err := exec.Command(
		"docker-compose",
		"--no-ansi",
		"-f", filepath.Join("compose", "gss", "docker-compose.yml"),
		"up",
		"--build",
		"--exit-code-from", "psql",
	).CombinedOutput()
	if err != nil {
		t.Log(string(out))
		t.Fatal(err)
	}
}

func TestComposeFlyway(t *testing.T) {
	out, err := exec.Command(
		"docker-compose",
		"-f", filepath.Join("compose", "flyway", "docker-compose.yml"),
		"up",
		"--force-recreate",
		"--exit-code-from", "flyway",
	).CombinedOutput()
	if err != nil {
		t.Log(string(out))
		t.Fatal(err)
	}
}
