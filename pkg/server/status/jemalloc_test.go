// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !stdmalloc

package status

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestJemalloc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	cgoAllocated, _, err := getJemallocStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		allocateMemory()
		cgoAllocatedN, _, err := getJemallocStats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if cgoAllocatedN == cgoAllocated {
			t.Errorf("allocated stat not incremented on allocation: %d", cgoAllocated)
		}
		cgoAllocated = cgoAllocatedN
	}
}
