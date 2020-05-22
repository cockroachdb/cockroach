// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !stdmalloc

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
