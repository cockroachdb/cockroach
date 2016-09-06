// Copyright 2016 The Cockroach Authors.
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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package mon

import (
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
	"golang.org/x/net/context"
)

func TestMemoryUsageMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	m := MemoryUsageMonitor{}
	ctx := context.Background()

	if err := m.reserveMemory(ctx, 10); err == nil {
		t.Fatal("monitor failed to reject non-monitored request")
	}

	m.StartMonitor()
	m.maxAllocatedBudget = 100

	if err := m.reserveMemory(ctx, 10); err != nil {
		t.Fatalf("monitor refused small allocation: %v", err)
	}
	if err := m.reserveMemory(ctx, 91); err == nil {
		t.Fatalf("monitor accepted excessive allocation: %v", err)
	}
	if err := m.reserveMemory(ctx, 90); err != nil {
		t.Fatalf("monitor refused top allocation: %v", err)
	}
	if m.curAllocated != 100 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.curAllocated, 100)
	}

	m.releaseMemory(ctx, 90) // Should succeed without panic.
	if m.curAllocated != 10 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.curAllocated, 10)
	}
	if m.maxAllocated != 100 {
		t.Fatalf("incorrect max allocation: got %d, expected %d", m.maxAllocated, 100)
	}

	m.releaseMemory(ctx, 10) // Should succeed without panic.
	if m.curAllocated != 0 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.curAllocated, 0)
	}
}
