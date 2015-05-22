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
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/util"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "output version information",
	Long: `
Output build version information.
`,
	Run: func(cmd *cobra.Command, args []string) {
		info := util.GetBuildInfo()
		w := &tabwriter.Writer{}
		w.Init(os.Stdout, 2, 1, 2, ' ', 0)
		fmt.Fprintf(w, "Build Vers:  %s\n", info.Vers)
		fmt.Fprintf(w, "Build Tag:   %s\n", info.Tag)
		fmt.Fprintf(w, "Build Time:  %s\n", info.Time)
		fmt.Fprintf(w, "Build Deps:\n\t%s\n",
			strings.Replace(strings.Replace(info.Deps, " ", "\n\t", -1), ":", "\t", -1))
	},
}

var cockroachCmd = &cobra.Command{
	Use: "cockroach",
}

func init() {
	cockroachCmd.AddCommand(
		initCmd,
		startCmd,
		certCmd,
		exterminateCmd,
		quitCmd,

		logCmd,

		kvCmd,
		acctCmd,
		permCmd,
		rangeCmd,
		zoneCmd,

		// Miscellaneous commands.
		// TODO(pmattis): stats
		versionCmd,
	)

	// The default cobra usage and help templates have some
	// ugliness. For example, the "Additional help topics:" section is
	// shown unnecessarily and it doesn't place a newline before the
	// "Flags:" section if there are no subcommands. We should really
	// get these tweaks merged upstream.
	cockroachCmd.SetUsageTemplate(`{{ $cmd := . }}Usage: {{if .Runnable}}
  {{.UseLine}}{{if .HasFlags}} [flags]{{end}}{{end}}{{if .HasSubCommands}}
  {{ .CommandPath}} [command]{{end}}{{if gt .Aliases 0}}

Aliases:
  {{.NameAndAliases}}
{{end}}{{if .HasExample}}

Examples:
{{ .Example }}{{end}}{{ if .HasRunnableSubCommands}}

Available Commands: {{range .Commands}}{{if and (.Runnable) (not .Deprecated)}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}
{{ if .HasLocalFlags}}
Flags:
{{.LocalFlags.FlagUsages}}{{end}}{{ if .HasInheritedFlags}}
Global Flags:
{{.InheritedFlags.FlagUsages}}{{end}}{{if .HasHelpSubCommands}}
Additional help topics:
{{if .HasHelpSubCommands}}{{range .Commands}}{{if and (not .Runnable) (not .Deprecated)}} {{rpad .CommandPath .CommandPathPadding}} {{.Short}}{{end}}{{end}}{{end}}
{{end}}{{ if .HasSubCommands }}
Use "{{.Root.Name}} help [command]" for more information about a command.
{{end}}`)
	cockroachCmd.SetHelpTemplate(`{{with or .Long .Short }}{{. | trim}}

{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}
{{end}}`)
}

// Run ...
func Run(args []string) error {
	cockroachCmd.SetArgs(args)
	return cockroachCmd.Execute()
}
