// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package main

import (
	"bytes"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestExport(t *testing.T) {
	testLabels := make(map[string]string)
	testLabels["some"] = "42test"
	testLabels["abc/def"] = "good/label?"
	ddFilePath := path.Join(datapathutils.TestDataPath(t), "export")
	datadriven.RunTest(t, ddFilePath, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "export" {
			d.Fatalf(t, "unknown command %s", d.Cmd)
		}
		testDir := datapathutils.TestDataPath(t, "reports", d.CmdArgs[0].String())
		writer := new(bytes.Buffer)
		err := exportMetrics(testDir, writer, timeutil.Unix(1684920350, 0), testLabels)
		require.NoError(t, err)

		// Return the output sorted by line.
		output := strings.Split(writer.String(), "\n")
		sort.Strings(output)
		return strings.Join(output, "\n")
	})
}

func TestSanitize(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{"test", "test"},
		{"test/sla//sh", "test_sla__sh"},
		{"5words", "_words"},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.output, sanitize(tc.input))
		})
	}
}
