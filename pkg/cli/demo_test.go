// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestDemo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCLITest(TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	defer democluster.TestingForceRandomizeDemoPorts()()

	setCLIDefaultsForTests()
	// We must reset the security asset loader here, otherwise the dummy
	// asset loader that is set by default in tests will not be able to
	// find the certs that demo sets up.
	securityassets.ResetLoader()
	defer ResetTest()

	// Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "demo", "test_demo"), func(t *testing.T, td *datadriven.TestData) string {
		if td.Cmd != "exec" {
			t.Fatalf("unsupported command: %s", td.Cmd)
		}
		cmd := strings.Split(td.Input, "\n")
		// Disable multi-tenant for this test due to the unsupported gossip commands.
		cmd = append(cmd, "--multitenant=false")
		cmd = append(cmd, "--logtostderr")
		log.TestingResetActive()
		out, err := c.RunWithCaptureArgs(cmd)
		if err != nil {
			t.Fatal(err)
		}
		// Skip the first line, since that just echoes the command.
		_, afterFirstLine, _ := strings.Cut(out, "\n")
		return afterFirstLine
	})
}
