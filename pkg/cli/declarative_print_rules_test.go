// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// This test doctoring a secure cluster.
func TestDeclarativeRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCLITest(TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	t.Run("declarative corpus validation standalone command", func(t *testing.T) {
		invalidOut, err := c.RunWithCapture(fmt.Sprintf("debug declarative-print-rules %s op", "1.1"))
		if err != nil {
			t.Fatal(err)
		}
		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "declarative-rules", "invalid_version"), func(t *testing.T, td *datadriven.TestData) string {
			// Do not display the present current version within the output,
			// for testing purposes. This can change from build to build, and
			// need changes for every version bump.
			return strings.Replace(invalidOut,
				" "+clusterversion.Latest.String()+"\n",
				" latest\n",
				-1)
		})

		depOut, err := c.RunWithCapture(fmt.Sprintf("debug declarative-print-rules %s dep", clusterversion.PreviousRelease))
		if err != nil {
			t.Fatal(err)
		}
		datadriven.RunTest(t, datapathutils.TestDataPath(t, "declarative-rules", "deprules"), func(t *testing.T, td *datadriven.TestData) string {
			return depOut
		})
	})
}
