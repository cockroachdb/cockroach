// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestTableDumpColumnParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.RunTest(t, "testdata/table_dump_column_parsing", func(t *testing.T, d *datadriven.TestData) string {
		table, ok := clusterWideTableDumps[d.Cmd]
		require.True(t, ok, "table dump not found: %s", d.Cmd)

		var buf bytes.Buffer
		for _, line := range strings.Split(strings.TrimSpace(d.Input), "\n") {
			cols := strings.Fields(strings.TrimSpace(line))
			fn, ok := table[cols[0]]
			require.True(t, ok, "column not found: %s", cols[0])

			decoded, err := fn(strings.TrimSpace(cols[1]))
			require.NoError(t, err)

			raw, err := json.Marshal(decoded)
			require.NoError(t, err)

			buf.Write(append(raw, '\n'))
		}

		return buf.String()
	})
}
