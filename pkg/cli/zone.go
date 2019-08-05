// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// A getZoneCmd command displays a zone config.
var getZoneCmd = &cobra.Command{
	Use:   "get - this command has been removed",
	Short: "this command has been removed",
	Long: `
This command has been removed.
Use SHOW ZONE CONFIGURATION FOR ... in a SQL client instead.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runGetZone),
}

// runGetZone retrieves the zone config for a given object id,
// and if present, outputs its YAML representation.
func runGetZone(_ *cobra.Command, _ []string) error {
	return errors.WithHint(
		errors.New(`command "get" has been removed`),
		"Use SHOW ZONE CONFIGURATION FOR ... in a SQL client instead.")
}

// A lsZonesCmd command displays a list of zone configs.
var lsZonesCmd = &cobra.Command{
	Use:   "ls - this command has been removed",
	Short: "this command has been removed",
	Long: `
This command has been removed.
Use SHOW ZONE CONFIGURATIONS in a SQL client instead.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runLsZones),
}

func runLsZones(_ *cobra.Command, _ []string) error {
	return errors.WithHint(
		errors.New(`command "ls" has been removed`),
		"Use SHOW ZONE CONFIGURATIONS in a SQL client instead.")
}

// A rmZoneCmd command removes a zone config.
var rmZoneCmd = &cobra.Command{
	Use:   "rm - this command has been removed",
	Short: "this command has been removed",
	Long: `
This command has been removed.
Use ALTER ... CONFIGURE ZONE DISCARD in a SQL client instead.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runRmZone),
}

func runRmZone(_ *cobra.Command, _ []string) error {
	return errors.WithHint(
		errors.New(`command "rm" has been removed`),
		"Use ALTER ... CONFIGURE ZONE DISCARD in a SQL client instead.")
}

// A setZoneCmd command creates a new or updates an existing zone config.
var setZoneCmd = &cobra.Command{
	Use:   "set - this command has been removed",
	Short: "this command has been removed",
	Long: `
This command has been removed.
Use ALTER ... CONFIGURE ZONE in a SQL client instead.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runSetZone),
}

// runSetZone parses the yaml input file, converts it to proto, and inserts it
// in the system.zones table.
func runSetZone(_ *cobra.Command, _ []string) error {
	return errors.WithHint(
		errors.New(`command "set" has been removed`),
		"Use ALTER ... CONFIGURE ZONE in a SQL client instead.")
}

var zoneCmds = []*cobra.Command{
	getZoneCmd,
	lsZonesCmd,
	rmZoneCmd,
	setZoneCmd,
}

var zoneCmd = &cobra.Command{
	Use:        "zone",
	Short:      "this command has been removed",
	RunE:       usageAndErr,
	Deprecated: "use SHOW ZONE and CONFIGURE ZONE commands in a SQL client instead.",
}

func init() {
	zoneCmd.AddCommand(zoneCmds...)
}
