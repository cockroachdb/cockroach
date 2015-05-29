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

package cli

import (
	"github.com/cockroachdb/cockroach/server"

	"github.com/spf13/cobra"
)

// A getPermsCmd command displays the perm config for the specified
// prefix.
var getPermsCmd = &cobra.Command{
	Use:   "get [options] <key-prefix>",
	Short: "fetches and displays the permission config",
	Long: `
Fetches and displays the permission configuration for <key-prefix>. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run: runGetPerms,
}

// runGetPerms invokes the REST API with GET action and key prefix as path.
func runGetPerms(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	server.RunGetPerm(Context, args[0])
}

// A lsPermsCmd command displays a list of perm configs by prefix.
var lsPermsCmd = &cobra.Command{
	Use:   "ls [options] [key-regexp]",
	Short: "list all permisison configs by key prefix",
	Long: `
List permission configs. If a regular expression is given, the results of
the listing are filtered by key prefixes matching the regexp. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run: runLsPerms,
}

// runLsPerms invokes the REST API with GET action and no path, which
// fetches a list of all perm configuration prefixes. The optional
// regexp is applied to the complete list and matching prefixes
// displayed.
func runLsPerms(cmd *cobra.Command, args []string) {
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	pattern := ""
	if len(args) == 1 {
		pattern = args[0]
	}
	server.RunLsPerm(Context, pattern)

}

// A rmPermsCmd command removes a perm config by prefix.
var rmPermsCmd = &cobra.Command{
	Use:   "rm [options] <key-prefix>",
	Short: "remove a permission config by key prefix",
	Long: `
Remove an existing permission config by key prefix. No action is taken if no
permission configuration exists for the specified key prefix. Note that this
command can affect only a single perm config with an exactly matching
prefix. The key prefix should be escaped via URL query escaping if it
contains non-ascii bytes or spaces.
`,
	Run: runRmPerms,
}

// runRmPerms invokes the REST API with DELETE action and key prefix as
// path.
func runRmPerms(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	server.RunRmPerm(Context, args[0])
}

// A setPermsCmd command creates a new or updates an existing perm
// config.
var setPermsCmd = &cobra.Command{
	Use:   "set [options] <key-prefix> <perm-config-file>",
	Short: "create or update permission config for key prefix",
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
	Run: runSetPerms,
}

// runSetPerm invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
func runSetPerms(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}

	server.RunSetPerm(Context, args[0], args[1])
}

// TODO:(bram) Add inline json for setting
// TODO:(bram) Add ability to add/remove a single user's read or write permission on a prefix

var permCmds = []*cobra.Command{
	getPermsCmd,
	lsPermsCmd,
	rmPermsCmd,
	setPermsCmd,
}

var permCmd = &cobra.Command{
	Use:   "perm",
	Short: "get, set, list and remove permissions",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

func init() {
	permCmd.AddCommand(permCmds...)
}
