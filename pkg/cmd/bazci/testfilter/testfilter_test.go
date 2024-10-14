// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testfilter

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func TestFilterAndWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			in := strings.NewReader(td.Input)
			var out strings.Builder
			if err := FilterAndWrite(in, &out, []string{td.Cmd}); err != nil {
				return err.Error()
			}
			// At the time of writing, datadriven garbles the test files when
			// rewriting a "\n" output, so make sure we never have trailing
			// newlines.
			return strings.TrimRight(out.String(), "\r\n")
		})
	})
}
