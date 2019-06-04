// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cli

import "github.com/spf13/cobra"

func init() {
	AddSubCmd(func(userFacing bool) *cobra.Command {
		var bashCmd = SetCmdDefaults(&cobra.Command{
			Use:   `gen-bash-completions <output-file>`,
			Short: `generate bash completions for workload command`,
			Args:  cobra.ExactArgs(1),
		})
		bashCmd.Run = func(cmd *cobra.Command, args []string) {
			for parent := cmd.Parent(); parent != nil; parent = cmd.Parent() {
				cmd = parent
			}
			if err := cmd.GenBashCompletionFile(args[0]); err != nil {
				panic(err)
			}
		}
		bashCmd.Hidden = userFacing
		return bashCmd
	})
}
