// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestParseDeclareSection(t *testing.T) {
	fn := `
DECLARE
BEGIN
  IMPORT TABLE employees
  FROM PGDUMP 's3://{BUCKET NAME}/{employees-full.sql}?AWS_ACCESS_KEY_ID={ACCESS KEY}&AWS_SECRET_ACCESS_KEY={SECRET ACCESS KEY}'
  WITH skip_foreign_keys WITH ignore_unsupported_statements;
END
`
	stmt, err := parser.Parse(fn)
	require.NoError(t, err)
	require.Equal(t, "DECLARE\nBEGIN\nEND\n", stmt.String())
}

func TestParseDataDriver(t *testing.T) {
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "parse":
				// Check parse.
				fn, err := parser.Parse(d.Input)
				if err != nil {
					d.Fatalf(t, "unexpected parse error: %v", err)
				}

				// TODO(chengxiong) add pretty print round trip test.
				return fn.String()
			}
			d.Fatalf(t, "unsupported command: %s", d.Cmd)
			return ""
		})
	})
}
