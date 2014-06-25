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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"flag"
	"os"

	commander "code.google.com/p/go-commander"
	"github.com/cockroachdb/cockroach/server"
)

func main() {
	c := commander.Commander{
		Name: "cockroach",
		Commands: []*commander.Command{
			server.CmdInit,
			server.CmdGetZone,
			server.CmdLsZones,
			server.CmdRmZone,
			server.CmdSetZone,
			server.CmdStart,
			&commander.Command{
				UsageLine: "listparams",
				Short:     "list all available parameters and their default values",
				Long: `
List all available parameters and their default values.
Note that parameter parsing stops after the first non-
option after the command name. Hence, the options need
to precede any additional arguments,

  cockroach <command> [options] [arguments].`,
				Run: func(cmd *commander.Command, args []string) {
					flag.CommandLine.PrintDefaults()
				},
			},
		},
	}

	if err := c.Run(os.Args[1:]); err != nil {
		os.Exit(1)
	}
}
