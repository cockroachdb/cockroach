// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestExecStartTemplate(t *testing.T) {
	startData1 := startTemplateData{
		LogDir: "./path with spaces/logs/$THIS_DOES_NOT_EVER_GET_EXPANDED",
		KeyCmd: `echo foo && \
echo bar $HOME`,
		EnvVars:   []string{"ROACHPROD=1/tigtag", "COCKROACH=foo", "ROCKCOACH=17%"},
		Binary:    "./cockroach",
		Args:      []string{`start`, `--log`, `file-defaults: {dir: '/path with spaces/logs', exit-on-error: false}`},
		MemoryMax: "81%",
		Local:     true,
	}
	datadriven.Walk(t, datapathutils.TestDataPath(t, "start"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			if td.Cmd != "data1" {
				t.Fatalf("unsupported")
			}
			out, err := execStartTemplate(startData1)
			require.NoError(t, err)
			return out
		})
	})
}
