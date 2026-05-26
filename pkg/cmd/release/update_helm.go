// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"os/exec"
)

// updateHelm runs commands in the helm-charts repo in order to update the cockroachdb version.
func updateHelm(workDir string, version string) error {
	// Helm charts use the version without the "v" prefix, but the bumper trims it if present.
	cmd := exec.Command("sh", "-c", fmt.Sprintf("make bump/%s", version))
	cmd.Dir = workDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed running '%s' with message '%s': %w", cmd.String(), string(out), err)
	}
	log.Printf("ran '%s': %s\n", cmd.String(), string(out))

	return nil
}
