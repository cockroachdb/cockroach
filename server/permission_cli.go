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

// A CmdGetPermission command displays the permission config for the specified
// prefix.
var CmdGetPermission = &commander.Command{
	UsageLine: "get-permission [options] <key-prefix>",
	Short:     "fetches and displays the permission config",
	Long: `
Fetches and displays the permission configuration for <key-prefix>. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run:  runGetPermission,
	Flag: *flag.CommandLine,
}

// runGetPermission invokes the REST API with GET action and key prefix as path.
func runGetPermission(cmd *commander.Command, args []string) {
	runGetConfig(permissionKeyPrefix, cmd, args)
}

// A CmdLsPermissions command displays a list of permission configs by prefix.
var CmdLsPermissions = &commander.Command{
	UsageLine: "ls-permissions [options] [key-regexp]",
	Short:     "list all permission configs by key prefix",
	Long: `
List permission configs. If a regular expression is given, the results of
the listing are filtered by key prefixes matching the regexp. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run:  runLsPermissions,
	Flag: *flag.CommandLine,
}

// runLsPermissions invokes the REST API with GET action and no path, which
// fetches a list of all permission configuration prefixes. The optional
// regexp is applied to the complete list and matching prefixes
// displayed.
func runLsPermissions(cmd *commander.Command, args []string) {
	runLsConfigs(permissionKeyPrefix, cmd, args)

}

// A CmdRmPermission command removes a permission config by prefix.
var CmdRmPermission = &commander.Command{
	UsageLine: "rm-permission [options] <key-prefix>",
	Short:     "remove a permission config by key prefix",
	Long: `
Remove an existing permission config by key prefix. No action is taken if no
permission configuration exists for the specified key prefix. Note that this
command can affect only a single permission config with an exactly matching
prefix. The key prefix should be escaped via URL query escaping if it
contains non-ascii bytes or spaces.
`,
	Run:  runRmPermission,
	Flag: *flag.CommandLine,
}

// runRmPermission invokes the REST API with DELETE action and key prefix as
// path.
func runRmPermission(cmd *commander.Command, args []string) {
	runRmConfig(permissionKeyPrefix, cmd, args)
}

// A CmdSetPermission command creates a new or updates an existing permission
// config.
var CmdSetPermission = &commander.Command{
	UsageLine: "set-permission [options] <key-prefix> <permission-config-file>",
	Short:     "create or update permission config for key prefix",
	Long: `
Create or update a permission config for the specified key prefix (first
argument: <key-prefix>) to the contents of the specified file
(second argument: <permission-config-file>). The key prefix should be
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
	Run:  runSetPermission,
	Flag: *flag.CommandLine,
}

// runSetPermission invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
func runSetPermission(cmd *commander.Command, args []string) {
	runSetConfig(permissionKeyPrefix, cmd, args)
}
