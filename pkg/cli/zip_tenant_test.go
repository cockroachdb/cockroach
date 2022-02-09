// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// Dummy import to pull in kvtenantccl. This allows us to start tenants.
// We need ccl functionality in order to test debug zip for tenant servers.
var _ = kvtenantccl.Connector{}

// TestTenantZip tests the operation of zip for a tenant server.
func TestTenantZip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderRace(t, "test too slow under race")

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := NewCLITest(TestCLIParams{
		StoreSpecs: []base.StoreSpec{{
			Path: dir,
		}},
		Multitenant: true,
		Insecure:    true,
	})
	defer c.Cleanup()

	out, err := c.RunWithCapture("debug zip --concurrency=1 --cpu-profile-duration=1s " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// We use datadriven simply to read the golden output file; we don't actually
	// run any commands. Using datadriven allows TESTFLAGS=-rewrite.
	datadriven.RunTest(t,
		testutils.TestDataPath(t, "zip", "testzip_tenant"),
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		},
	)
}
