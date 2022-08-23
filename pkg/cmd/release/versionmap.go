// Copyright 2022 The Cockroach Authors.
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
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/spf13/cobra"
)

var versionMapFlags = struct {
	file       string
	versionStr string
}{}

var versionMapCmd = &cobra.Command{
	Use:   "update-version-map",
	Short: "Update roachtest version map",
	Long:  "Updates the version map used in the version-upgrade roachtest acceptance test",
	RunE:  updateVersionMap,
}

func init() {
	versionMapCmd.Flags().StringVar(&versionMapFlags.file, "version-map-file",
		"pkg/cmd/roachtest/tests/predecessor_version.json", "version map json file")
	versionMapCmd.Flags().StringVar(&versionMapFlags.versionStr, versionFlag, "", "cockroachdb version")
	_ = versionMapCmd.MarkFlagRequired(versionFlag)
}

type versionMap map[string]string

func updateVersionMap(_ *cobra.Command, _ []string) error {
	// make sure we have the leading "v" in the version
	versionMapFlags.versionStr = "v" + strings.TrimPrefix(versionMapFlags.versionStr, "v")
	var err error
	version, err := semver.NewVersion(versionMapFlags.versionStr)
	if err != nil {
		return fmt.Errorf("cannot parse version %s: %w", updateVersionsFlags.versionStr, err)
	}
	content, err := os.ReadFile(versionMapFlags.file)
	if err != nil {
		return fmt.Errorf("cannot open %s: %w", versionMapFlags.file, err)
	}
	var verMap versionMap
	err = json.Unmarshal(content, &verMap)
	if err != nil {
		return fmt.Errorf("cannot unmarshal %s: %w", versionMapFlags.file, err)
	}
	nextSeries := nextReleaseSeries(version)
	// If there is no entry in the version map, probably this is the major version release case,
	// where we also should update the fixtures. Bail for now.
	if _, ok := verMap[nextSeries]; !ok {
		return fmt.Errorf("cannot create a new major release entry for %s in verMap", nextSeries)
	}
	// The version upgrade version map uses versions without the "v" prefix.
	// Replace the previous version with the current one. Note instead of using Original(),
	// which returns the version with a leading "v", we use String() with returns the version without it.
	verMap[nextSeries] = version.String()
	out, err := json.MarshalIndent(verMap, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshal: %w", err)
	}
	out = append(out, '\n')
	err = os.WriteFile(versionMapFlags.file, out, 0644)
	if err != nil {
		return fmt.Errorf("cannot write version map to %s: %w", versionMapFlags.file, err)
	}
	return nil
}

// nextReleaseSeries parses the version and returns the next release series assuming we have 2 releases yearly
func nextReleaseSeries(version *semver.Version) string {
	nextMinor := version.IncMinor()
	// TODO(rail): revisit when we have more than 2 releases a year
	if nextMinor.Minor() > 2 {
		nextMinor = nextMinor.IncMajor()
		// IncMajor() resets all version parts to 0, thus we need to bump the minor part to match our version schema.
		nextMinor = nextMinor.IncMinor()
	}
	return fmt.Sprintf("%d.%d", nextMinor.Major(), nextMinor.Minor())
}
