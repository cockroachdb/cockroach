// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"os"

	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/spf13/cobra"
)

// WorkloadCmd returns a new command that can serve as the root of the workload
// command tree.
func WorkloadCmd(userFacing bool) *cobra.Command {
	rootCmd := SetCmdDefaults(&cobra.Command{
		Use:   `workload`,
		Short: `generators for data and query loads`,
	})
	for _, subCmdFn := range subCmdFns {
		rootCmd.AddCommand(subCmdFn(userFacing))
	}
	if userFacing {
		allowlist := map[string]struct{}{
			`workload`: {},
			`init`:     {},
			`run`:      {},
		}
		for _, m := range workload.Registered() {
			allowlist[m.Name] = struct{}{}
		}
		var addExperimental func(c *cobra.Command)
		addExperimental = func(c *cobra.Command) {
			c.Short = `[experimental] ` + c.Short
			if _, ok := allowlist[c.Name()]; !ok {
				c.Hidden = true
			}
			for _, sub := range c.Commands() {
				addExperimental(sub)
			}
		}
		addExperimental(rootCmd)
	}
	return rootCmd
}

var subCmdFns []func(userFacing bool) *cobra.Command

// AddSubCmd adds a sub-command closure to the workload cli.
//
// Most commands are global singletons and hooked together in init functions but
// the workload ones do fancy things with making each generator in
// `workload.Registered` into a subcommand. This means the commands need to be
// generated after `workload.Registered` is fully populated, but the latter is
// now sometimes done in the outermost possible place (main.go) and so its init
// runs last. Instead, we return a closure that is called in the main function,
// which is guaranteed to run after all inits.
func AddSubCmd(fn func(userFacing bool) *cobra.Command) {
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
