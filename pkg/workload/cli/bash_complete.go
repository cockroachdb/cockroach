// Copyright 2019 The Cockroach Authors.
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
