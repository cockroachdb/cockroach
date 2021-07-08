// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanlatch"
)

// LatchMetrics holds information about the state of a latchManager.
type LatchMetrics = spanlatch.Metrics

// LockTableMetrics holds information about the state of a lockTable.
type LockTableMetrics struct {
	Locks                 int64
	LocksHeld             int64
	LocksHeldByDurability [lock.MaxDurability + 1]int64
	LocksWithReservation  int64
	LocksWithWaitQueues   int64

	Waiters                  int64
	WaitingReaders           int64
	WaitingWriters           int64
	MaxWaitersForLock        int64
	MaxWaitingReadersForLock int64
	MaxWaitingWritersForLock int64
}
