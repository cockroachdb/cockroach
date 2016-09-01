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
	"github.com/cockroachdb/cockroach/util/syncutil"
	"golang.org/x/net/context"
)

// MemoryUsageMonitorWithMutex encapsulates a MemoryUsageMonitor using
// a Mutex.
type MemoryUsageMonitorWithMutex struct {
	syncutil.Mutex
	mon MemoryUsageMonitor
}

// StartMonitor is a pass-through wrapper with locking.
func (mu *MemoryUsageMonitorWithMutex) StartMonitor(pool *MemoryPool, acc MemoryAccount) {
	mu.Lock()
	defer mu.Unlock()
	mu.mon.StartMonitor(pool, acc)
}

// StartUnlimitedMonitor is a pass-through wrapper with locking.
func (mu *MemoryUsageMonitorWithMutex) StartUnlimitedMonitor() {
	mu.Lock()
	defer mu.Unlock()
	mu.mon.StartUnlimitedMonitor()
}

// GrowAccount is a pass-through wrapper with locking.
func (mu *MemoryUsageMonitorWithMutex) GrowAccount(ctx context.Context, acc *MemoryAccount, x int64) error {
	mu.Lock()
	defer mu.Unlock()
	return mu.mon.GrowAccount(ctx, acc, x)
}

// CloseAccount is a pass-through wrapper with locking.
func (mu *MemoryUsageMonitorWithMutex) CloseAccount(ctx context.Context, acc *MemoryAccount) {
	mu.Lock()
	defer mu.Unlock()
	mu.mon.CloseAccount(ctx, acc)
}
