// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"fmt"
	"path"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	//lint:ignore SA1019 benchstat is deprecated
	"golang.org/x/perf/benchstat"
)

func tablesToText(tablesMap map[string][]*benchstat.Table) string {
	buf := new(bytes.Buffer)
	for sheet, tables := range tablesMap {
		// Sort the tables by benchmark name so that the output is deterministic.
		for _, table := range tables {
			sort.Slice(table.Rows, func(i, j int) bool {
				return table.Rows[i].Benchmark < table.Rows[j].Benchmark
			})
		}
		if buf.Len() != 0 {
			fmt.Fprintf(buf, "\n")
		}
		fmt.Fprintf(buf, "Sheet: %s\n", sheet)
		benchstat.FormatText(buf, tables)
	}
	return buf.String()
}

func TestCompareBenchmarks(t *testing.T) {
	ddFilePath := path.Join(datapathutils.TestDataPath(t), "compare")
	datadriven.RunTest(t, ddFilePath, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd == "compare" {
			src := datapathutils.TestDataPath(t, "reports", d.CmdArgs[0].String())
			dst := datapathutils.TestDataPath(t, "reports", d.CmdArgs[1].String())
			packages, err := getPackagesFromLogs(src)
			require.NoError(t, err)
			tables, err := compareBenchmarks(packages, src, dst)
			require.NoError(t, err)
			return tablesToText(tables)
		}
		return ""
	})
}
