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
	Use:   "get [options] <database[.table]>",
	Short: "fetches and displays the zone config",
	Long: `
Fetches and displays the zone configuration for the specified database or
table.
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
	Use:   "ls [options]",
	Short: "list all zone configs",
	Long: `
List zone configs.
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
	Use:   "rm [options] <database[.table]>",
	Short: "remove a zone config",
	Long: `
Remove an existing zone config for the specified database or table.
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
	Use:   "set [options] <database[.table]> -f file.yaml",
	Short: "create or update zone config for object ID",
	Long: `
Create or update the zone config for the specified database or table to the
specified zone-config from the given file ("-" for stdin).

The zone config format has the following YAML schema:

  num_replicas: <num>
  constraints: [comma-separated attribute list]
  range_min_bytes: <size-in-bytes>
  range_max_bytes: <size-in-bytes>
  gc:
    ttlseconds: <time-in-seconds>

For example, to set the zone config for the system database, run:
$ cockroach zone set system -f - << EOF
num_replicas: 3
constraints: [ssd, -mem]
EOF

Note that the specified zone config is merged with the existing zone config for
the database or table.
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
	Short:      "get, set, list and remove zones",
	RunE:       usageAndErr,
	Deprecated: "use SHOW ZONE and CONFIGURE ZONE commands in a SQL client instead.",
}

func init() {
	zoneCmd.AddCommand(zoneCmds...)
}
