// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter.mattis@gmail.com)

package cli

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/util"

	commander "code.google.com/p/go-commander"
)

var listParamsCmd = &commander.Command{
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
}

var versionCmd = &commander.Command{
	UsageLine: "version",
	Short:     "output version information",
	Long: `
Output build version information.
`,
	Run: func(cmd *commander.Command, args []string) {
		info := util.GetBuildInfo()
		w := &tabwriter.Writer{}
		w.Init(os.Stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(w, "Build Vers:  %s\n", info.Vers)
		fmt.Fprintf(w, "Build Tag:   %s\n", info.Tag)
		fmt.Fprintf(w, "Build Time:  %s\n", info.Time)
		fmt.Fprintf(w, "Build Deps:\n\t%s\n",
			strings.Replace(strings.Replace(info.Deps, " ", "\n\t", -1), ":", "\t", -1))
		w.Flush()
	},
}

var allCmds = &commander.Commander{
	Name: "cockroach",
	Commands: []*commander.Command{
		// Node commands.
		initCmd,
		startCmd,
		exterminateCmd,
		quitCmd,

		// Certificate commands.
		createCACertCmd,
		createNodeCertCmd,

		// Key/value commands.
		getCmd,
		putCmd,
		incCmd,
		delCmd,
		scanCmd,

		// Range commands.
		lsRangesCmd,
		splitRangeCmd,
		mergeRangeCmd,

		// Accounting commands.
		getAcctCmd,
		lsAcctsCmd,
		rmAcctCmd,
		setAcctCmd,

		// Permission commands.
		getPermsCmd,
		lsPermsCmd,
		rmPermsCmd,
		setPermsCmd,

		// Zone commands.
		getZoneCmd,
		lsZonesCmd,
		rmZoneCmd,
		setZoneCmd,

		// Miscellaneous commands.
		// TODO(pmattis): stats
		listParamsCmd,
		versionCmd,
	},
}

// Run ...
func Run(args []string) error {
	return allCmds.Run(args)
}
