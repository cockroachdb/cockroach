// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowcontainer

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	defer serverutils.TestingGlobalDRPCOption(base.TestDRPCEnabledRandomly)()
	os.Exit(m.Run())
}
