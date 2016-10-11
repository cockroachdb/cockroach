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

	"github.com/cockroachdb/cockroach/build"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
)

// Proxy to allow overrides in tests.
var osStderr = os.Stderr

var versionIncludesDeps bool

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "output version information",
	Long: `
Output build version information.
`,
	Run: func(cmd *cobra.Command, args []string) {
		info := build.GetInfo()
		tw := tabwriter.NewWriter(os.Stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(tw, "Build Tag:   %s\n", info.Tag)
		fmt.Fprintf(tw, "Build Time:  %s\n", info.Time)
		fmt.Fprintf(tw, "Platform:    %s\n", info.Platform)
		fmt.Fprintf(tw, "Go Version:  %s\n", info.GoVersion)
		fmt.Fprintf(tw, "C Compiler:  %s\n", info.CgoCompiler)
		if versionIncludesDeps {
			fmt.Fprintf(tw, "Build Deps:\n\t%s\n",
				strings.Replace(strings.Replace(info.Dependencies, " ", "\n\t", -1), ":", "\t", -1))
		}
		_ = tw.Flush()
	},
}

var cockroachCmd = &cobra.Command{
	Use:   "cockroach [command] (flags)",
	Short: "CockroachDB command-line interface and server",
	// TODO(cdo): Add a pointer to the docs in Long.
	Long: `CockroachDB command-line interface and server.`,
}

// isInteractive indicates whether both stdin and stdout refer to the
// terminal.
var isInteractive = isatty.IsTerminal(os.Stdout.Fd()) &&
	isatty.IsTerminal(os.Stdin.Fd())

func init() {
	cobra.EnableCommandSorting = false

	cockroachCmd.AddCommand(
		startCmd,
		certCmd,
		freezeClusterCmd,
		quitCmd,

		sqlShellCmd,
		userCmd,
		zoneCmd,
		nodeCmd,
		dumpCmd,

		// Miscellaneous commands.
		// TODO(pmattis): stats
		genCmd,
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
