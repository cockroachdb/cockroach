// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/test/framework"
)

// TestRoachprodVersion verifies the roachprod version command works
func TestRoachprodVersion(t *testing.T) {
	rpt := framework.NewRoachprodTest(t, framework.DisableCleanup())

	rpt.RunExpectSuccess("version")
}
