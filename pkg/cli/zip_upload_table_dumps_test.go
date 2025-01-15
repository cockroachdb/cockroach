// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableDumpColumnParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.RunTest(t, "testdata/table_dump_column_parsing", func(t *testing.T, d *datadriven.TestData) string {
		table, ok := clusterWideTableDumps[d.Cmd]
		if !ok {
			table, ok = nodeSpecificTableDumps[d.Cmd]
		}
		require.True(t, ok, "table dump not found: %s", d.Cmd)

		t.Log(table)

		var buf bytes.Buffer
		for _, line := range strings.Split(strings.TrimSpace(d.Input), "\n") {
			cols := strings.Fields(strings.TrimSpace(line))
			fn, ok := table[cols[0]]
			if !ok {
				buf.WriteString(strings.TrimSpace(cols[1]) + "\n")
				continue
			}

			decoded, err := fn(strings.TrimSpace(cols[1]))
			require.NoError(t, err)

			raw, err := json.Marshal(decoded)
			require.NoError(t, err)

			buf.Write(append(raw, '\n'))
		}

		return buf.String()
	})
}

func TestTableDumpConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for table := range clusterWideTableDumps {
		if strings.Contains(table, "fallback") {
			continue
		}

		table = strings.TrimSuffix(table, ".txt")
		_, perCluster := zipInternalTablesPerCluster[table]
		_, perClusterAcrossDBs := zipInternalTablesPerCluster[fmt.Sprintf(`"".%s`, table)]
		_, system := zipSystemTables[table]
		assert.True(t, perCluster || system || perClusterAcrossDBs, "table %s is not in table registry", table)
	}

	for table := range nodeSpecificTableDumps {
		if strings.Contains(table, "fallback") {
			continue
		}

		table = strings.TrimSuffix(table, ".txt")
		_, ok := zipInternalTablesPerNode[table]
		assert.True(t, ok, "table %s is not in table registry", table)
	}
}

func TestMakeTableIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tt := []struct {
		name    string
		input   io.Reader
		headers []string
	}{
		{
			name:    "simple",
			input:   bytes.NewBufferString("h1\th2\th3\nr1c1\tr1c2\tr1c3\nr2c1\tr2c2\tr2c3\n"),
			headers: []string{"h1", "h2", "h3"},
		},
		{
			name: "token too long",
			input: bytes.NewBufferString(fmt.Sprintf(
				"h1\n%s\n", strings.Repeat("A", bufio.MaxScanTokenSize+1),
			)),
			headers: []string{"h1"},
		},
	}

	for _, tc := range tt {
		headers, iter := makeTableIterator(tc.input)
		assert.Equal(t, tc.headers, headers)
		require.NoError(t, iter(func(row string) error {
			assert.Len(t, strings.Split(row, "\t"), len(headers))
			return nil
		}))
	}
}
