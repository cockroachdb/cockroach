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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package server

import (
	"flag"

	commander "code.google.com/p/go-commander"
)

// A CmdGetPerms command displays the perm config for the specified
// prefix.
var CmdGetPerms = &commander.Command{
	UsageLine: "get-perms [options] <key-prefix>",
	Short:     "fetches and displays the permission config",
	Long: `
Fetches and displays the permission configuration for <key-prefix>. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run:  runGetPerms,
	Flag: *flag.CommandLine,
}

// runGetPerms invokes the REST API with GET action and key prefix as path.
func runGetPerms(cmd *commander.Command, args []string) {
	runGetConfig(permKeyPrefix, cmd, args)
}

// A CmdLsPerms command displays a list of perm configs by prefix.
var CmdLsPerms = &commander.Command{
	UsageLine: "ls-perms [options] [key-regexp]",
	Short:     "list all permisison configs by key prefix",
	Long: `
List permission configs. If a regular expression is given, the results of
the listing are filtered by key prefixes matching the regexp. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run:  runLsPerms,
	Flag: *flag.CommandLine,
}

// runLsPerms invokes the REST API with GET action and no path, which
// fetches a list of all perm configuration prefixes. The optional
// regexp is applied to the complete list and matching prefixes
// displayed.
func runLsPerms(cmd *commander.Command, args []string) {
	runLsConfigs(permKeyPrefix, cmd, args)

}

// A CmdRmPerms command removes a perm config by prefix.
var CmdRmPerms = &commander.Command{
	UsageLine: "rm-perms [options] <key-prefix>",
	Short:     "remove a permission config by key prefix",
	Long: `
Remove an existing permission config by key prefix. No action is taken if no
permission configuration exists for the specified key prefix. Note that this
command can affect only a single perm config with an exactly matching
prefix. The key prefix should be escaped via URL query escaping if it
contains non-ascii bytes or spaces.
`,
	Run:  runRmPerms,
	Flag: *flag.CommandLine,
}

// runRmPerms invokes the REST API with DELETE action and key prefix as
// path.
func runRmPerms(cmd *commander.Command, args []string) {
	runRmConfig(permKeyPrefix, cmd, args)
}

// A CmdSetPerms command creates a new or updates an existing perm
// config.
var CmdSetPerms = &commander.Command{
	UsageLine: "set-perm [options] <key-prefix> <perm-config-file>",
	Short:     "create or update permission config for key prefix",
	Long: `
Create or update a perm config for the specified key prefix (first
argument: <key-prefix>) to the contents of the specified file
(second argument: <perm-config-file>). The key prefix should be
escaped via URL query escaping if it contains non-ascii bytes or
spaces.

The permission config format has the following YAML schema:

  read:
    - user1
    - user2
    - ...
  write:
  	- user1
  	- user2
  	- ...

For example:

  read:
    - readOnlyUser
    - readWriteUser
  write:
    - readWriteUser
    - WriteOnlyUser

Setting permission configs will guarantee that users will have permissions for
this key prefix and all sub prefixes of the one that is set
`,
	Run:  runSetPerms,
	Flag: *flag.CommandLine,
}

// runSetPerm invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
func runSetPerms(cmd *commander.Command, args []string) {
	runSetConfig(permKeyPrefix, cmd, args)
}

// TODO:(bram) Add inline json for setting
// TODO:(bram) Add ability to add/remove a single user's read or write permission on a prefix
