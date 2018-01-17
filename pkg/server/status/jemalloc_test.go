// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
