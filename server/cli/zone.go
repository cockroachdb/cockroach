// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package cli

import (
	"github.com/cockroachdb/cockroach/server"

	"github.com/spf13/cobra"
)

// A getZoneCmd command displays the zone config for the specified
// prefix.
var getZoneCmd = &cobra.Command{
	Use:   "get [options] <key-prefix>",
	Short: "fetches and displays the zone config",
	Long: `
Fetches and displays the zone configuration for <key-prefix>. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run: runGetZone,
}

// runGetZone invokes the REST API with GET action and key prefix as path.
func runGetZone(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	server.RunGetZone(Context, args[0])
}

// A lsZonesCmd command displays a list of zone configs by prefix.
var lsZonesCmd = &cobra.Command{
	Use:   "ls [options] [key-regexp]",
	Short: "list all zone configs by key prefix",
	Long: `
List zone configs. If a regular expression is given, the results of
the listing are filtered by key prefixes matching the regexp. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run: runLsZones,
}

// runLsZones invokes the REST API with GET action and no path, which
// fetches a list of all zone configuration prefixes. The optional
// regexp is applied to the complete list and matching prefixes
// displayed.
func runLsZones(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	pattern := ""
	if len(args) == 1 {
		pattern = args[0]
	}
	server.RunLsZone(Context, pattern)
}

// A rmZoneCmd command removes a zone config by prefix.
var rmZoneCmd = &cobra.Command{
	Use:   "rm [options] <key-prefix>",
	Short: "remove a zone config by key prefix",
	Long: `
Remove an existing zone config by key prefix. No action is taken if no
zone configuration exists for the specified key prefix. Note that this
command can affect only a single zone config with an exactly matching
prefix. The key prefix should be escaped via URL query escaping if it
contains non-ascii bytes or spaces.
`,
	Run: runRmZone,
}

// runRmZone invokes the REST API with DELETE action and key prefix as
// path.
func runRmZone(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	server.RunRmZone(Context, args[0])
}

// A setZoneCmd command creates a new or updates an existing zone
// config.
var setZoneCmd = &cobra.Command{
	Use:   "set [options] <key-prefix> <zone-config-file>",
	Short: "create or update zone config for key prefix",
	Long: `
Create or update a zone config for the specified key prefix (first
argument: <key-prefix>) to the contents of the specified file
(second argument: <zone-config-file>). The key prefix should be
escaped via URL query escaping if it contains non-ascii bytes or
spaces.

The zone config format has the following YAML schema:

  replicas:
    - attrs: [comma-separated attribute list]
    - attrs:  ...
  range_min_bytes: <size-in-bytes>
  range_max_bytes: <size-in-bytes>

For example:

  replicas:
    - attrs: [us-east-1a, ssd]
    - attrs: [us-east-1b, ssd]
    - attrs: [us-west-1b, ssd]
  range_min_bytes: 8388608
  range_max_bytes: 67108864

Setting zone configs will guarantee that key ranges will be split
such that no key range straddles two zone config specifications.
This feature can be taken advantage of to pre-split ranges.
`,
	Run: runSetZone,
}

// runSetZone invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
func runSetZone(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	server.RunSetZone(Context, args[0], args[1])
}

var zoneCmds = []*cobra.Command{
	getZoneCmd,
	lsZonesCmd,
	rmZoneCmd,
	setZoneCmd,
}

var zoneCmd = &cobra.Command{
	Use:   "zone",
	Short: "get, set, list and remove zones\n",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

func init() {
	zoneCmd.AddCommand(zoneCmds...)
}
