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
// permissions and limitations under the License.

package cli

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var sqlfmtCmd *cobra.Command = &cobra.Command{
	Use:   "sqlfmt",
	Short: "format SQL statements",
	Long:  "Formats SQL statements from stdin to line length n.",
	RunE:  runSQLFmt,
}

func runSQLFmt(cmd *cobra.Command, args []string) error {
	if sqlfmtCtx.len < 1 {
		return errors.Errorf("line length must be > 0: %d", sqlfmtCtx.len)
	}
	if sqlfmtCtx.tabWidth < 1 {
		return errors.Errorf("tab width must be > 0: %d", sqlfmtCtx.tabWidth)
	}

	var sl tree.StatementList
	if len(sqlfmtCtx.execStmts) != 0 {
		for _, exec := range sqlfmtCtx.execStmts {
			stmts, err := parser.Parse(exec)
			if err != nil {
				return err
			}
			sl = append(sl, stmts...)
		}
	} else {
		in, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		sl, err = parser.Parse(string(in))
		if err != nil {
			return err
		}
	}

	for _, s := range sl {
		fmt.Print(tree.PrettyWithOpts(s, sqlfmtCtx.len, !sqlfmtCtx.useSpaces, sqlfmtCtx.tabWidth, !sqlfmtCtx.noSimplify))
		if len(sl) > 1 {
			fmt.Print(";")
		}
		fmt.Println()
	}
	return nil
}
