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
	"flag"

	commander "code.google.com/p/go-commander"
	"github.com/cockroachdb/cockroach/server"
)

// TODO:(bram) change this api to not require a file, just set (no file),
//   get(true/false), ls, rm

// A CmdGetAcct command displays the acct config for the specified
// prefix.
var CmdGetAcct = &commander.Command{
	UsageLine: "get-acct [options] <key-prefix>",
	Short:     "fetches and displays an accounting config",
	Long: `
Fetches and displays the accounting configuration for <key-prefix>. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run:  runGetAcct,
	Flag: *flag.CommandLine,
}

// runGetAcct invokes the REST API with GET action and key prefix as path.
func runGetAcct(cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	server.RunGetAcct(Context, args[0])
}

// A CmdLsAccts command displays a list of acct configs by prefix.
var CmdLsAccts = &commander.Command{
	UsageLine: "ls-accts [options] [key-regexp]",
	Short:     "list all accounting configs by key prefix",
	Long: `
List accounting configs. If a regular expression is given, the results of
the listing are filtered by key prefixes matching the regexp. The key
prefix should be escaped via URL query escaping if it contains
non-ascii bytes or spaces.
`,
	Run:  runLsAccts,
	Flag: *flag.CommandLine,
}

// runLsAccts invokes the REST API with GET action and no path, which
// fetches a list of all acct configuration prefixes. The optional
// regexp is applied to the complete list and matching prefixes
// displayed.
func runLsAccts(cmd *commander.Command, args []string) {
	if len(args) > 1 {
		cmd.Usage()
		return
	}
	pattern := ""
	if len(args) == 1 {
		pattern = args[0]
	}
	server.RunLsAcct(Context, pattern)

}

// A CmdRmAcct command removes an acct config by prefix.
var CmdRmAcct = &commander.Command{
	UsageLine: "rm-acct [options] <key-prefix>",
	Short:     "remove an accounting config by key prefix",
	Long: `
Remove an existing accounting config by key prefix. No action is taken if no
accounting configuration exists for the specified key prefix. Note that this
command can affect only a single accounting config with an exactly matching
prefix. The key prefix should be escaped via URL query escaping if it
contains non-ascii bytes or spaces.
`,
	Run:  runRmAcct,
	Flag: *flag.CommandLine,
}

// runRmAcct invokes the REST API with DELETE action and key prefix as
// path.
func runRmAcct(cmd *commander.Command, args []string) {
	if len(args) != 1 {
		cmd.Usage()
		return
	}
	server.RunRmAcct(Context, args[0])
}

// A CmdSetAcct command creates a new or updates an existing acct
// config.
var CmdSetAcct = &commander.Command{
	UsageLine: "set-acct [options] <key-prefix> <acct-config-file>",
	Short:     "create or update an accounting config for key prefix",
	Long: `
Create or update a accounting config for the specified key prefix (first
argument: <key-prefix>) to the contents of the specified file
(second argument: <acct-config-file>). The key prefix should be
escaped via URL query escaping if it contains non-ascii bytes or
spaces.

The accounting config format has the following YAML schema:

  cluster_id: cluster

For example:

  cluster_id: test
`,
	Run:  runSetAcct,
	Flag: *flag.CommandLine,
}

// runSetAcct invokes the REST API with POST action and key prefix as
// path. The specified configuration file is read from disk and sent
// as the POST body.
func runSetAcct(cmd *commander.Command, args []string) {
	if len(args) != 2 {
		cmd.Usage()
		return
	}
	server.RunSetAcct(Context, args[0], args[1])
}

// TODO:(bram) Add inline json for setting
