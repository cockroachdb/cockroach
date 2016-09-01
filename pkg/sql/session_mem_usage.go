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

package sql

import (
	"math"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/mon"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

// OpenSessionAccount interfaces between Session and mon.MemoryMonitor.
func (s *Session) OpenSessionAccount() WrappableMemoryAccount {
	res := WrappableMemoryAccount{}
	s.mon.OpenAccount(s.context, &res.acc)
	return res
}

// OpenTxnAccount interfaces between Session and mon.MemoryMonitor.
func (s *Session) OpenTxnAccount() WrappableMemoryAccount {
	res := WrappableMemoryAccount{}
	s.txnMon.OpenAccount(s.TxnState.Ctx, &res.acc)
	return res
}

// WrappableMemoryAccount encapsulates a MemoryAccount to
// give it the Wsession()/Wtxn() method below.
type WrappableMemoryAccount struct {
	acc mon.MemoryAccount
}

// Wsession captures the current session monitor pointer and session
// logging context so they can be provided transparently to the other
// Account APIs below.
func (w *WrappableMemoryAccount) Wsession(s *Session) WrappedMemoryAccount {
	return WrappedMemoryAccount{
		acc: &w.acc,
		mon: &s.sessionMon,
		ctx: s.context,
	}
}

// Wtxn captures the current txn-specific monitor pointer and session
// logging context so they can be provided transparently to the other
// Account APIs below.
func (w *WrappableMemoryAccount) Wtxn(s *Session) WrappedMemoryAccount {
	return WrappedMemoryAccount{
		acc: &w.acc,
		mon: &s.txnMon,
		ctx: s.TxnState.Ctx,
	}
}

// WrappedMemoryAccount is the transient structure that carries
// the extra argument to the MemoryAccount APIs.
type WrappedMemoryAccount struct {
	acc *mon.MemoryAccount
	mon *mon.MemoryMonitor
	ctx context.Context
}

// OpenAndInit interfaces between Session and mon.MemoryMonitor.
func (w WrappedMemoryAccount) OpenAndInit(initialAllocation int64) error {
	return w.mon.OpenAndInitAccount(w.ctx, w.acc, initialAllocation)
}

// Grow interfaces between Session and mon.MemoryMonitor.
func (w WrappedMemoryAccount) Grow(extraSize int64) error {
	return w.mon.GrowAccount(w.ctx, w.acc, extraSize)
}

// Close interfaces between Session and mon.MemoryMonitor.
func (w WrappedMemoryAccount) Close() {
	w.mon.CloseAccount(w.ctx, w.acc)
}

// Clear interfaces between Session and mon.MemoryMonitor.
func (w WrappedMemoryAccount) Clear() {
	w.mon.ClearAccount(w.ctx, w.acc)
}

// ResizeItem interfaces between Session and mon.MemoryMonitor.
func (w WrappedMemoryAccount) ResizeItem(oldSize, newSize int64) error {
	return w.mon.ResizeItem(w.ctx, w.acc, oldSize, newSize)
}

// noteworthyMemoryUsageBytes is the minimum size tracked by a
// transaction or session monitor before the monitor starts explicitly
// logging overall usage growth in the log.
var noteworthyMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_SESSION_MEMORY_USAGE", 10*1024)

// StartMonitor interfaces between Session and mon.MemoryMonitor
func (s *Session) StartMonitor(pool *mon.MemoryMonitor, reserved mon.BoundAccount) {
	// Note: we pass `reserved` to s.mon where it causes `s.mon` to act
	// as a buffer. This is not done for sessionMon nor TxnState.mon:
	// these monitors don't start with any buffer, so they'll need to
	// ask their "parent" for memory as soon as the first
	// allocation. This is acceptable because the session is single
	// threaded, and the point of buffering is just to avoid contention.
	s.mon = mon.MakeMonitor("root",
		s.memMetrics.CurBytesCount,
		s.memMetrics.MaxBytesHist,
		-1, math.MaxInt64)
	s.mon.Start(s.context, pool, reserved)
	s.deriveAndStartMonitors()
}

// StartUnlimitedMonitor interfaces between Session and mon.MemoryMonitor
func (s *Session) StartUnlimitedMonitor() {
	s.mon = mon.MakeUnlimitedMonitor(s.context,
		"root",
		s.memMetrics.CurBytesCount,
		s.memMetrics.MaxBytesHist,
		math.MaxInt64,
	)
	s.deriveAndStartMonitors()
}

func (s *Session) deriveAndStartMonitors() {
	s.sessionMon = mon.MakeMonitor("session",
		s.memMetrics.SessionCurBytesCount,
		s.memMetrics.SessionMaxBytesHist,
		-1, noteworthyMemoryUsageBytes)
	s.sessionMon.Start(s.context, &s.mon, mon.BoundAccount{})

	// We merely prepare the txn monitor here. It is fully started in
	// resetForNewSQLTxn().
	s.txnMon = mon.MakeMonitor("txn",
		s.memMetrics.TxnCurBytesCount,
		s.memMetrics.TxnMaxBytesHist,
		-1, noteworthyMemoryUsageBytes)
}
