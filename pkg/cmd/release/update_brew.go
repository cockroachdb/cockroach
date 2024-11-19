// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/Masterminds/semver/v3"
)

// updateBrew runs commands in the homebrew-tap repo in order to update the cockroachdb version.
func updateBrew(workDir string, version *semver.Version, latestMajor bool) error {
	// cockroach@major.minor is supported for all releases
	commands := []*exec.Cmd{
		exec.Command("make", fmt.Sprintf("VERSION=%s", version.String()), fmt.Sprintf("PRODUCT=cockroach@%d.%d", version.Major(), version.Minor())),
	}
	// limited to the latest release only
	if latestMajor {
		commands = append(commands, exec.Command("make", fmt.Sprintf("VERSION=%s", version.String()), "PRODUCT=cockroach"))
		commands = append(commands, exec.Command("make", fmt.Sprintf("VERSION=%s", version.String()), "PRODUCT=cockroach-sql"))
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
