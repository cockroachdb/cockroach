// Copyright 2018 The Cockroach Authors.
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

package cli

import (
	"os"

	"github.com/spf13/cobra"
)

// WorkloadCmd returns a new command that can serve as the root of the workload
// command tree.
func WorkloadCmd() *cobra.Command {
	rootCmd := SetCmdDefaults(&cobra.Command{
		Use:   `workload`,
		Short: `generators for data and query loads`,
	})
	for _, subCmdFn := range subCmdFns {
		rootCmd.AddCommand(subCmdFn())
	}
	return rootCmd
}

var subCmdFns []func() *cobra.Command

// AddSubCmd adds a sub-command closure to the workload cli.
//
// Most commands are global singletons and hooked together in init functions but
// the workload ones do fancy things with making each generator in
// `workload.Registered` into a subcommand. This means the commands need to be
// generated after `workload.Registered` is fully populated, but the latter is
// now sometimes done in the outermost possible place (main.go) and so its init
// runs last. Instead, we return a closure that is called in the main function,
// which is guaranteed to run after all inits.
func AddSubCmd(fn func() *cobra.Command) {
	subCmdFns = append(subCmdFns, fn)
}

// HandleErrs wraps a `RunE` cobra function to print an error but not the usage.
func HandleErrs(
	f func(cmd *cobra.Command, args []string) error,
) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		err := f(cmd, args)
		if err != nil {
			cmd.Println("Error:", err.Error())
			os.Exit(1)
		}
	}
}
