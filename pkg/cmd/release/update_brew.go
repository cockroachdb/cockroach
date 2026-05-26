// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/cockroachdb/version"
)

// updateBrew runs commands in the homebrew-tap repo in order to update the cockroachdb version.
func updateBrew(workDir string, ver version.Version, latestMajor bool) error {
	// cockroach@major.minor is supported for all releases
	commands := []*exec.Cmd{
		exec.Command("make", ver.Format("VERSION=%X.%Y.%Z"), ver.Format("PRODUCT=cockroach@%X.%Y")),
	}
	// limited to the latest release only
	if latestMajor {
		commands = append(commands, exec.Command("make", ver.Format("VERSION=%X.%Y.%Z"), "PRODUCT=cockroach"))
		commands = append(commands, exec.Command("make", ver.Format("VERSION=%X.%Y.%Z"), "PRODUCT=cockroach-sql"))
	}
	commands = append(commands, exec.Command("git", "add", "Formula"))
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
