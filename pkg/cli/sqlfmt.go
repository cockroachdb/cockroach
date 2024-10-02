// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// TODO(mjibson): This subcommand has more flags than I would prefer. My
// goal is to have it have just -e and nothing else. gofmt initially started
// with tabs/spaces and width specifiers but later regretted that decision
// and removed them. I would like to get to the point where we achieve SQL
// formatting nirvana and we make this an opinionated formatter with few or
// zero options, hopefully before 2.1.

var sqlfmtCmd = &cobra.Command{
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

	var sl statements.Statements
	if len(sqlfmtCtx.execStmts) != 0 {
		for _, exec := range sqlfmtCtx.execStmts {
			stmts, err := parser.Parse(exec)
			if err != nil {
				return err
			}
			sl = append(sl, stmts...)
		}
	} else {
		in, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		sl, err = parser.Parse(string(in))
		if err != nil {
			return err
		}
	}

	cfg := tree.DefaultPrettyCfg()
	cfg.UseTabs = !sqlfmtCtx.useSpaces
	cfg.LineWidth = sqlfmtCtx.len
	cfg.TabWidth = sqlfmtCtx.tabWidth
	cfg.Simplify = !sqlfmtCtx.noSimplify
	cfg.Align = tree.PrettyNoAlign
	cfg.JSONFmt = true
	if sqlfmtCtx.align {
		cfg.Align = tree.PrettyAlignAndDeindent
	}

	for i := range sl {
		p, err := cfg.Pretty(sl[i].AST)
		if err != nil {
			return err
		}
		fmt.Print(p)
		if len(sl) > 1 {
			fmt.Print(";")
		}
		fmt.Println()
	}
	return nil
}
