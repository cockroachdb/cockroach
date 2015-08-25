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
// Author: Marc berhault (marc@cockroachlabs.com)

package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

// sqlShellCmd opens a sql shell.
var sqlShellCmd = &cobra.Command{
	Use:   "sql [options]",
	Short: "open a sql shell",
	Long: `
Open a sql shell running against the cockroach database at --addr.
`,
	Run: runTerm,
}

func runTerm(cmd *cobra.Command, args []string) {
	if len(args) != 0 {
		cmd.Usage()
		return
	}

	db := makeSQLClient()

	liner := liner.NewLiner()
	defer func() {
		_ = liner.Close()
	}()

	for {
		// TODO(marc): handle multi-line, this will require ';' terminated statements.
		l, err := liner.Prompt("> ")
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Input error: %s\n", err)
			}
			break
		}
		if len(l) == 0 {
			continue
		}
		liner.AppendHistory(l)

		if err := runQuery(db, l); err != nil {
			fmt.Printf("Error: %s\n", err)
		}
	}
}
