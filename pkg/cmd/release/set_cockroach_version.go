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
	"os"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"
)

const versionFile = "pkg/build/version.txt"

var setCockroachVersionFlags = struct {
	versionStr string
}{}

var setCockroachVersionCmd = &cobra.Command{
	Use:   "set-cockroach-version",
	Short: "Sets the cockroach version to a given version",
	Long:  "Updates the version.txt file to set the cockroach version to a user-provided version",
	RunE:  setCockroachVersion,
}

func init() {
	setCockroachVersionCmd.Flags().StringVar(&setCockroachVersionFlags.versionStr, versionFlag, "", "cockroachdb version")
	_ = setCockroachVersionCmd.MarkFlagRequired(versionFlag)
}

func setCockroachVersion(_ *cobra.Command, _ []string) error {
	// validate the version given
	_, err := semver.NewVersion(setCockroachVersionFlags.versionStr)
	if err != nil {
		return fmt.Errorf("cannot parse version %s: %w", setCockroachVersionFlags.versionStr, err)
	}
	return updateVersionFile(versionFile, setCockroachVersionFlags.versionStr)
}

func updateVersionFile(dest string, version string) error {
	contents := []byte(version + "\n")
	if err := os.WriteFile(dest, contents, 0644); err != nil {
		return fmt.Errorf("cannot write version.txt: %w", err)
	}
	return nil
}
