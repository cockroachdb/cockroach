// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

//go:generate ../util/leaktest/add-leaktest.sh *_test.go

func TestMain(m *testing.M) {
	randutil.SeedForTests()

	os.Exit(m.Run())
}
