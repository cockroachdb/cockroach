// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// +build !stdmalloc

package status

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestJemalloc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
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
