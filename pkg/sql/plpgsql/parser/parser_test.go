// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser_test

import (
	"testing"

	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree/utils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/datadriven"
)

// TestParseDataDriven verifies that we can parse the supplied PL/pgSQL.
//
// The follow commands are allowed:
//
//   - parse
//
//     Parses PL/pgSQL and verifies that it round-trips. Various forms of the
//     formatted AST are printed as test output.
//
//   - error
//
//     Parses PL/pgSQL and expects an error. The error is printed as test
//     output.
//
//   - feature-count
//
//     Parses PL/pgSQL and prints PL/pgSQL-related telemetry counters.
func TestParseDataDriven(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "parse":
				reParseWithoutLiterals := true
				for _, arg := range d.CmdArgs {
					if arg.Key == "no-parse-without-literals" {
						reParseWithoutLiterals = false
					}
				}
				return sqlutils.VerifyParseFormat(t, d.Input, d.Pos, sqlutils.PLpgSQL, reParseWithoutLiterals)
			case "error":
				_, err := plpgsql.Parse(d.Input)
				if err == nil {
					d.Fatalf(t, "%s\nexpected error, found none", d.Pos)
				}
				return sqlutils.VerifyParseError(err)
			case "feature-count":
				fn, err := utils.CountPLpgSQLStmt(d.Input)
				if err != nil {
					d.Fatalf(t, "%s\nunexpected parse error: %v", d.Pos, err)
				}
				return fn.String()
			}
			d.Fatalf(t, "%s\nunsupported command: %s", d.Pos, d.Cmd)
			return ""
		})
	})
}
