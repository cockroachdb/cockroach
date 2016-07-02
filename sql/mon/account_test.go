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

func TestMemAcc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	m := MemoryUsageMonitor{}
	ctx := context.Background()

	var a1, a2 MemoryAccount

	m.StartMonitor()
	m.OpenAccount(ctx, &a1)
	m.OpenAccount(ctx, &a2)

	m.maxAllocatedBudget = 100

	if err := m.GrowAccount(ctx, &a1, 10); err != nil {
		t.Fatalf("monitor refused allocation: %v", err)
	}

	if err := m.GrowAccount(ctx, &a2, 30); err != nil {
		t.Fatalf("monitor refused allocation: %v", err)
	}

	if err := m.GrowAccount(ctx, &a1, 61); err == nil {
		t.Fatalf("monitor accepted excessive allocation")
	}

	if err := m.GrowAccount(ctx, &a2, 61); err == nil {
		t.Fatalf("monitor accepted excessive allocation")
	}

	m.ClearAccount(ctx, &a1)

	if err := m.GrowAccount(ctx, &a2, 61); err != nil {
		t.Fatalf("monitor refused allocation: %v", err)
	}

	if err := m.ResizeItem(ctx, &a2, 50, 60); err == nil {
		t.Fatalf("monitor accepted excessive allocation")
	}

	if err := m.ResizeItem(ctx, &a1, 0, 5); err != nil {
		t.Fatalf("monitor refused allocation: %v", err)
	}

	if err := m.ClearAccountAndAlloc(ctx, &a2, 40); err != nil {
		t.Fatalf("monitor refused reset + allocation: %v", err)
	}

	m.CloseAccount(ctx, &a1)
	m.CloseAccount(ctx, &a2)

	if m.curAllocated != 0 {
		t.Fatal("closing spans leaves bytes in monitor")
	}
}
