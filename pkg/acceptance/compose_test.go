// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	 http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package acceptance

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestComposeGSS(t *testing.T) {
	out, err := exec.Command(
		"docker-compose",
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
