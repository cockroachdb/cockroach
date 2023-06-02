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
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

func TestExport(t *testing.T) {
	testLabels := make(map[string]string)
	testLabels["some"] = "test"
	testLabels["abc/def"] = "good/label?"
	ddFilePath := path.Join(datapathutils.TestDataPath(t), "export")
	datadriven.RunTest(t, ddFilePath, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "export" {
			d.Fatalf(t, "unknown command %s", d.Cmd)
		}
		testDir := datapathutils.TestDataPath(t, "reports", d.CmdArgs[0].String())
		writer := new(bytes.Buffer)
		err := exportMetrics(testDir, writer, "1684920350", testLabels)
		require.NoError(t, err)
		return writer.String()
	})
}
