// Copyright 2017 The Cockroach Authors.
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

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var cmds []*cobra.Command
var quiet bool

func main() {
	rootCmd := func() *cobra.Command {
		cmd := &cobra.Command{
			Use:   "docgen",
			Short: "docgen generates documentation for cockroachdb's SQL functions and grammar",
		}
		cmd.PersistentFlags().BoolVarP(&quiet, "quiet", "q", false, "suppress output where possible")
		cmd.AddCommand(cmds...)
		return cmd
	}()
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
