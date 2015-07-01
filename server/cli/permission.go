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
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package cli

import (
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/util/log"

	"github.com/spf13/cobra"
)

// A getPermsCmd command displays the perm config for the specified
// prefix.
var getPermsCmd = &cobra.Command{
	Use:   "get [options] <key-prefix>",
	Short: "fetches and displays the permission config",
	Long: `
Fetches and displays the permission configuration for <key-prefix>.
`,
	Run: runGetPerms,
}

// runGetPerms invokes the REST API with GET action and key prefix as path.
func runGetPerms(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	client := client.NewAdminClient(&Context.Context, Context.Addr, client.Permission)
	body, err := client.GetYAML(args[0])
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Permission config for prefix %q:\n%s\n", args[0], body)
}

// A lsPermsCmd command displays a list of perm configs by prefix.
var lsPermsCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all permisison configs by key prefix",
	Long: `
List permission configs.
`,
	Run: runLsPerms,
}

// runLsPerms invokes the REST API with GET action and no path, which
// fetches a list of all perm configuration prefixes.
func runLsPerms(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		cmd.Usage()
		return
	}
	client := client.NewAdminClient(&Context.Context, Context.Addr, client.Permission)
	list, err := client.List()
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Permission keys:\n%s\n", strings.Join(list, "\n  "))
}

// A rmPermsCmd command removes a perm config by prefix.
var rmPermsCmd = &cobra.Command{
	Use:   "rm [options] <key-prefix>",
	Short: "remove a permission config by key prefix",
	Long: `
Remove an existing permission config by key prefix. No action is taken if no
permission configuration exists for the specified key prefix. Note that this
command can affect only a single perm config with an exactly matching
prefix.
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
	client := client.NewAdminClient(&Context.Context, Context.Addr, client.Permission)
	if err := client.Delete(args[0]); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Deleted permission key %q\n", args[0])
}

// A setPermsCmd command creates a new or updates an existing perm
// config.
var setPermsCmd = &cobra.Command{
	Use:   "set [options] <key-prefix> <perm-config-file>",
	Short: "create or update permission config for key prefix",
	Long: `
Create or update a perm config for the specified key prefix (first
argument: <key-prefix>) to the contents of the specified file
(second argument: <perm-config-file>).

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
	// Read in the config file.
	body, err := ioutil.ReadFile(args[1])
	if err != nil {
		log.Errorf("unable to read permission config file %q: %s", args[1], err)
		return
	}
	client := client.NewAdminClient(&Context.Context, Context.Addr, client.Permission)
	if err := client.SetYAML(args[0], string(body)); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Wrote permission config to %q\n", args[0])
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
