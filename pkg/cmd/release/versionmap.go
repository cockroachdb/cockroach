// Copyright 2019 The Cockroach Authors.
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
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var versionMapFlags = struct {
	file    string
	version string
}{}

var versionMapCmd = &cobra.Command{
	Use:   "update-version-map",
	Short: "Update roachtest version map",
	Long:  "Updates the version map used in the version-upgrade roachtest acceptance test",
	RunE:  updateVersionMap,
}

func init() {
	versionMapCmd.Flags().StringVar(&versionMapFlags.file, "version-map-file",
		"pkg/cmd/roachtest/tests/predecessor_version.json", "cockroachdb version")
	versionMapCmd.Flags().StringVar(&versionMapFlags.version, versionFlag, "", "cockroachdb version")
	_ = versionMapCmd.MarkFlagRequired(versionFlag)
}

type versionMap map[string]string

func updateVersionMap(_ *cobra.Command, _ []string) error {
	content, err := ioutil.ReadFile(versionMapFlags.file)
	if err != nil {
		return fmt.Errorf("cannot open %s: %w", versionMapFlags.file, err)
	}
	var verMap versionMap
	err = json.Unmarshal(content, &verMap)
	if err != nil {
		return fmt.Errorf("cannot unmarshal %s: %w", versionMapFlags.file, err)
	}
	// The version upgrade version map uses versions without the "v" prefix
	version := strings.TrimPrefix(versionMapFlags.version, "v")
	nextSeries, err := nextReleaseSeries(version)
	if err != nil {
		return fmt.Errorf("cannot determine next release series for %s: %w", version, err)
	}
	// If there is no entry in the version map, probably this is the major version release case,
	// where we also should update the fixtures. Bail for now.
	if _, ok := verMap[nextSeries]; !ok {
		return fmt.Errorf("cannot create a new major release entry for %s in verMap", nextSeries)
	}
	// replace the previous version with the current one
	verMap[nextSeries] = version
	out, err := json.MarshalIndent(verMap, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshal: %w", err)
	}
	out = append(out, '\n')
	err = ioutil.WriteFile(versionMapFlags.file, out, 0644)
	if err != nil {
		return fmt.Errorf("cannot write version map to %s: %w", versionMapFlags.file, err)
	}
	return nil
}

// nextReleaseSeries parses the version and returns the next release series assuming we have 2 releases yearly
func nextReleaseSeries(version string) (string, error) {
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return "", fmt.Errorf("cannot use %s with less than 3 parts", version)
	}
	first, err := strconv.Atoi(parts[0])
	if err != nil {
		return "", fmt.Errorf("cannot parse %s first part", version)
	}
	second, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", fmt.Errorf("cannot parse %s second part", version)
	}
	second++
	// TODO(rail): revisit when we have more than 2 releases a year
	if second > 2 {
		second = 1
		first++
	}
	return fmt.Sprintf("%d.%d", first, second), nil
}
