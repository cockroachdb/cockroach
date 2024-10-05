// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqltestutils

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestSetDataDriven(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "large_schema"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return testSetDataDriven(t, d)
		})
	})
}

func testSetDataDriven(t *testing.T, d *datadriven.TestData) string {
	switch d.Cmd {
	case "generate":
		params := GenerateViewBasedGraphSchemaParams{}
		for _, arg := range d.CmdArgs {
			var err error
			require.Equal(t, len(arg.Vals), 1, "need 1 arg")
			switch arg.Key {
			case "SchemaName":
				params.SchemaName = arg.Vals[0]
			case "NumTablesPerDepth":
				params.NumTablesPerDepth, err = strconv.Atoi(arg.Vals[0])
				require.NoError(t, err)
			case "NumColumnsPerTable":
				params.NumColumnsPerTable, err = strconv.Atoi(arg.Vals[0])
				require.NoError(t, err)
			case "GraphDepth":
				params.GraphDepth, err = strconv.Atoi(arg.Vals[0])
				require.NoError(t, err)
			}
		}
		statements, err := GenerateViewBasedGraphSchema(params)
		require.NoError(t, err)
		resultBuffer := strings.Builder{}
		for _, stmt := range statements {
			resultBuffer.WriteString(stmt.SQL)
			resultBuffer.WriteString("\n")
		}
		return resultBuffer.String()
	default:
		t.Fatalf("unknown command %q", d.Cmd)
		panic("unreachable")
	}
}
