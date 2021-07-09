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
	"path/filepath"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestExecStartTemplate(t *testing.T) {
	startData1 := startTemplateData{
		LogDir: "./path with spaces/logs/$THIS_DOES_NOT_EVER_GET_EXPANDED",
		KeyCmd: `echo foo && \
echo bar $HOME`,
		Tag:       "tigtag",
		EnvVars:   []string{"COCKROACH=foo", "ROCKCOACH=17%"},
		Binary:    "./cockroach",
		StartCmd:  "start-single-node",
		Args:      []string{`--log "file-defaults: {dir: '/path with spaces/logs', exit-on-error: false}"`},
		MemoryMax: "81%",
		NodeNum:   1,
		Local:     true,
	}
	datadriven.Walk(t, filepath.Join("testdata", "start"), func(t *testing.T, path string) {
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
