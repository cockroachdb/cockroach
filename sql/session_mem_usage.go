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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/sql/mon"
)

// OpenAccount interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) OpenAccount() WrappableMemoryAccount {
	res := WrappableMemoryAccount{}
	s.mon.OpenAccount(s.Ctx(), &res.acc)
	return res
}

// WrappableMemoryAccount encapsulates a MemoryAccount to
// give it the W() method below.
type WrappableMemoryAccount struct {
	acc mon.MemoryAccount
}

// W captures the current monitor pointer and session logging context
// so they can be provided transparently to the other Account APIs
// below.
func (w *WrappableMemoryAccount) W(s *Session) WrappedMemoryAccount {
	return WrappedMemoryAccount{
		acc: &w.acc,
		mon: &s.mon,
		ctx: s.Ctx(),
	}
}

// WrappedMemoryAccount is the transient structure that carries
// the extra argument to the MemoryAccount APIs.
type WrappedMemoryAccount struct {
	acc *mon.MemoryAccount
	mon *mon.MemoryUsageMonitor
	ctx context.Context
}

// OpenAndInit interfaces between Session and mon.MemoryUsageMonitor.
func (w WrappedMemoryAccount) OpenAndInit(initialAllocation int64) error {
	return w.mon.OpenAndInitAccount(w.ctx, w.acc, initialAllocation)
}

// Grow interfaces between Session and mon.MemoryUsageMonitor.
func (w WrappedMemoryAccount) Grow(extraSize int64) error {
	return w.mon.GrowAccount(w.ctx, w.acc, extraSize)
}

// Close interfaces between Session and mon.MemoryUsageMonitor.
func (w WrappedMemoryAccount) Close() {
	w.mon.CloseAccount(w.ctx, w.acc)
}

// Clear interfaces between Session and mon.MemoryUsageMonitor.
func (w WrappedMemoryAccount) Clear() {
	w.mon.ClearAccount(w.ctx, w.acc)
}

// ResizeItem interfaces between Session and mon.MemoryUsageMonitor.
func (w WrappedMemoryAccount) ResizeItem(oldSize, newSize int64) error {
	return w.mon.ResizeItem(w.ctx, w.acc, oldSize, newSize)
}
