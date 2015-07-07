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

// TODO:(bram) change this api to not require a file, just set (no file),
//   get(true/false), ls, rm

// A getAcctCmd command displays the acct config for the specified
// prefix.
var getAcctCmd = &cobra.Command{
	Use:   "get [options] <key-prefix>",
	Short: "fetches and displays an accounting config",
	Long: `
Fetches and displays the accounting configuration for <key-prefix>.
`,
	Run: runGetAcct,
}

// runGetAcct invokes the REST API with GET action and key prefix as path.
func runGetAcct(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	admin := client.NewAdminClient(&Context.Context, Context.Addr, client.Accounting)
	body, err := admin.GetYAML(args[0])
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Accounting config for prefix %q:\n%s\n", args[0], body)
}

// A lsAcctsCmd command displays a list of acct configs by prefix.
var lsAcctsCmd = &cobra.Command{
	Use:   "ls [options]",
	Short: "list all accounting configs by key prefix",
	Long: `
List accounting configs.
`,
	Run: runLsAccts,
}

// runLsAccts invokes the REST API with GET action and no path, which
// fetches a list of all acct configuration prefixes.
func runLsAccts(cmd *cobra.Command, args []string) {
	if len(args) > 0 {
		cmd.Usage()
		return
	}
	admin := client.NewAdminClient(&Context.Context, Context.Addr, client.Accounting)
	list, err := admin.List()
	if err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Accounting keys:\n%s\n", strings.Join(list, "\n  "))
}

// A rmAcctCmd command removes an acct config by prefix.
var rmAcctCmd = &cobra.Command{
	Use:   "rm [options] <key-prefix>",
	Short: "remove an accounting config by key prefix",
	Long: `
Remove an existing accounting config by key prefix. No action is taken if no
accounting configuration exists for the specified key prefix. Note that this
command can affect only a single accounting config with an exactly matching
prefix.
`,
	Run: runRmAcct,
}

// runRmAcct invokes the REST API with DELETE action and key prefix as
// path.
func runRmAcct(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	admin := client.NewAdminClient(&Context.Context, Context.Addr, client.Accounting)
	if err := admin.Delete(args[0]); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Deleted accounting key %q\n", args[0])
}

// A setAcctCmd command creates a new or updates an existing acct
// config.
var setAcctCmd = &cobra.Command{
	Use:   "set [options] <key-prefix> <acct-config-file>",
	Short: "create or update an accounting config for key prefix",
	Long: `
Create or update an accounting config for the specified key prefix (first
argument: <key-prefix>) to the contents of the specified file
(second argument: <acct-config-file>).

The accounting config format has the following YAML schema:

  cluster_id: cluster

For example:

  cluster_id: test
`,
	Run: runSetAcct,
}

// runSetAcct invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
func runSetAcct(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	// Read in the config file.
	body, err := ioutil.ReadFile(args[1])
	if err != nil {
		log.Errorf("unable to read accounting config file %q: %s", args[1], err)
		return
	}
	admin := client.NewAdminClient(&Context.Context, Context.Addr, client.Accounting)
	if err := admin.SetYAML(args[0], string(body)); err != nil {
		log.Error(err)
		return
	}
	fmt.Printf("Wrote accounting config to %q\n", args[0])
}

// TODO:(bram) Add inline json for setting

var acctCmds = []*cobra.Command{
	getAcctCmd,
	lsAcctsCmd,
	rmAcctCmd,
	setAcctCmd,
}

var acctCmd = &cobra.Command{
	Use:   "acct",
	Short: "get, set, list and remove accounting configuration",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

func init() {
	acctCmd.AddCommand(acctCmds...)
}
