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
	"github.com/cockroachdb/cockroach/util/log"
	"golang.org/x/net/context"
)

func TestMemoryUsageMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	log.SetGlobalVerbosity(2)

	m := MemoryUsageMonitor{}

	if err := m.ReserveMemory(10); err == nil {
		t.Fatal("monitor failed to reject non-monitored request")
	}

	m.StartMonitor(context.Background(), "hello")
	m.maxAllocatedBudget = 100

	if err := m.ReserveMemory(10); err != nil {
		t.Fatal("monitor refused small allocation")
	}
	if err := m.ReserveMemory(91); err == nil {
		t.Fatal("monitor accepted excessive allocation")
	}
	if err := m.ReserveMemory(90); err != nil {
		t.Fatal("monitor refused top allocation")
	}
	if m.curAllocated != 100 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.curAllocated, 100)
	}

	m.ReleaseMemory(90) // Should succeed without panic.
	if m.curAllocated != 10 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.curAllocated, 10)
	}
	if m.maxAllocated != 100 {
		t.Fatalf("incorrect max allocation: got %d, expected %d", m.maxAllocated, 100)
	}

	m.ReleaseMemory(10) // Should succeed without panic.
	if m.curAllocated != 0 {
		t.Fatalf("incorrect current allocation: got %d, expected %d", m.curAllocated, 0)
	}
}
