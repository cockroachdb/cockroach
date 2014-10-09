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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package server

import (
	"flag"

	commander "code.google.com/p/go-commander"
)

// A CmdGetZone command displays the zone config for the specified
// prefix.
var CmdGetZone = &commander.Command{
	UsageLine: "get-zone [options] <key-prefix>",
	Short:     "fetches and displays the zone config",
	Long: `
Fetches and displays the zone configuration for <key-prefix>. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run:  runGetZone,
	Flag: *flag.CommandLine,
}

// runGetZone invokes the REST API with GET action and key prefix as path.
func runGetZone(cmd *commander.Command, args []string) {
	runGetConfig(zoneKeyPrefix, cmd, args)
}

// A CmdLsZones command displays a list of zone configs by prefix.
var CmdLsZones = &commander.Command{
	UsageLine: "ls-zones [options] [key-regexp]",
	Short:     "list all zone configs by key prefix",
	Long: `
List zone configs. If a regular expression is given, the results of
the listing are filtered by key prefixes matching the regexp. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run:  runLsZones,
	Flag: *flag.CommandLine,
}

// runLsZones invokes the REST API with GET action and no path, which
// fetches a list of all zone configuration prefixes. The optional
// regexp is applied to the complete list and matching prefixes
// displayed.
func runLsZones(cmd *commander.Command, args []string) {
	runLsConfigs(zoneKeyPrefix, cmd, args)
}

// A CmdRmZone command removes a zone config by prefix.
var CmdRmZone = &commander.Command{
	UsageLine: "rm-zone [options] <key-prefix>",
	Short:     "remove a zone config by key prefix",
	Long: `
Remove an existing zone config by key prefix. No action is taken if no
zone configuration exists for the specified key prefix. Note that this
command can affect only a single zone config with an exactly matching
prefix. The key prefix should be escaped via URL query escaping if it
contains non-ascii bytes or spaces.
`,
	Run:  runRmZone,
	Flag: *flag.CommandLine,
}

// runRmZone invokes the REST API with DELETE action and key prefix as
// path.
func runRmZone(cmd *commander.Command, args []string) {
	runRmConfig(zoneKeyPrefix, cmd, args)
}

// A CmdSetZone command creates a new or updates an existing zone
// config.
var CmdSetZone = &commander.Command{
	UsageLine: "set-zone [options] <key-prefix> <zone-config-file>",
	Short:     "create or update zone config for key prefix",
	Long: `
Create or update a zone config for the specified key prefix (first
argument: <key-prefix>) to the contents of the specified file
(second argument: <zone-config-file>). The key prefix should be
escaped via URL query escaping if it contains non-ascii bytes or
spaces.

The zone config format has the following YAML schema:

  replicas:
    - [comma-separated attribute list]
    - ...
  range_min_bytes: <size-in-bytes>
  range_max_bytes: <size-in-bytes>

For example:

  replicas:
    - [us-east-1a, ssd]
    - [us-east-1b, ssd]
    - [us-west-1b, ssd]
  range_min_bytes: 8388608
  range_min_bytes: 67108864

Setting zone configs will guarantee that key ranges will be split
such that no key range straddles two zone config specifications.
This feature can be taken advantage of to pre-split ranges.
`,
	Run:  runSetZone,
	Flag: *flag.CommandLine,
}

// runSetZone invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
func runSetZone(cmd *commander.Command, args []string) {
	runSetConfig(zoneKeyPrefix, cmd, args)
}
