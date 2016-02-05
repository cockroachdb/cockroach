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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/util"
)

// Proxy to allow overrides in tests.
var osStderr = os.Stderr

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "output version information",
	Long: `
Output build version information.
`,
	Run: func(cmd *cobra.Command, args []string) {
		info := util.GetBuildInfo()
		tw := tabwriter.NewWriter(os.Stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(tw, "Build Vers:  %s\n", info.Vers)
		fmt.Fprintf(tw, "Build Tag:   %s\n", info.Tag)
		fmt.Fprintf(tw, "Build Time:  %s\n", info.Time)
		fmt.Fprintf(tw, "Build Deps:\n\t%s\n",
			strings.Replace(strings.Replace(info.Deps, " ", "\n\t", -1), ":", "\t", -1))
		_ = tw.Flush()
	},
}

var cockroachCmd = &cobra.Command{
	Use: "cockroach",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		context.Addr = util.EnsureHostPort(context.Addr, util.CockroachPort)
		context.PGAddr = util.EnsureHostPort(context.PGAddr, util.PgPort)
	},
}

func init() {
	cockroachCmd.AddCommand(
		startCmd,
		certCmd,
		exterminateCmd,
		quitCmd,

		logCmd,

		sqlShellCmd,
		kvCmd,
		userCmd,
		rangeCmd,
		zoneCmd,
		nodeCmd,

		// Miscellaneous commands.
		// TODO(pmattis): stats
		versionCmd,
		debugCmd,
	)
}

// Run ...
func Run(args []string) error {
	cockroachCmd.SetArgs(args)
	return cockroachCmd.Execute()
}

func mustUsage(cmd *cobra.Command) {
	if err := cmd.Usage(); err != nil {
		panic(err)
	}
}
