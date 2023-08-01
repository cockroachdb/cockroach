// Copyright 2023 The Cockroach Authors.
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
	"fmt"
	"log"
	"os/exec"

	"github.com/Masterminds/semver/v3"
)

func updateHelm(workDir string, version *semver.Version) error {
	commands := []*exec.Cmd{
		exec.Command("bazel", "build", "//build"),
		exec.Command("sh", "-c", fmt.Sprintf("$(bazel info bazel-bin)/build/build_/build bump %s", version.String())),
	}
	for _, cmd := range commands {
		cmd.Dir = workDir
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed running '%s' with message '%s': %w", cmd.String(), string(out), err)
		}
		log.Printf("ran '%s': %s\n", cmd.String(), string(out))
	}
	return nil
}
