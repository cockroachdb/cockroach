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

import "github.com/cockroachdb/cockroach/sql/mon"

// Currently we bind an instance of MemoryUsageMonitor to each
// session, and the logical span for tracking memory usage is
// also bound to the entire duration of the session.
// Alternatives were considered and rejected:
//
// - binding to a single statement: fails to track transaction
//   state including intents across a transaction.
// - binding to a single transaction attempt: idem.
// - binding to an entire transaction: fails to track the
//   ResultList created by Executor.ExecuteStatements which
//   stays alive after the transaction commits and until
//   pgwire sends the ResultList back to the client.
// - binding to the duration of v3.go:handleExecute(): fails
//   to track transaction state that spans across multiple
//   separate execute messages.
//
// Ideally we would want a "magic" context that spans automatically
// from the start of a transaction to the point where all related
// Results (ResultList items) have been sent back to the
// client. However with this definition and the current code there can
// be multiple such "magic" contexts alive simultaneously. This is
// because a client can start a new transaction before it reads the
// ResultList of a previous transaction, e.g. if issuing `BEGIN;
// SELECT; COMMIT; BEGIN; SELECT; COMMIT` in one pgwire message.
//
// A way forward to implement this "magic" context would be to
// fix/implement #7775 (stream results from Executor to pgwire) and
// take care that the corresponding new streaming/pipeline logic
// passes a transaction-bound context to the monitor throughout.

// TODO(andrei): this file exists temporarily until a better
// log/tracing contexts is defined that tracks the lifecycle of
// AllocationSpans better than Session.

// CloseSpan interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) CloseSpan(span *mon.AllocationSpan) {
	s.mon.CloseSpan(span, s.Ctx())
}

// ResetSpan interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) ResetSpan(span *mon.AllocationSpan) {
	s.mon.ResetSpan(span, s.Ctx())
}

// ResetSpanAndAlloc interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) ResetSpanAndAlloc(span *mon.AllocationSpan, newSize int64) error {
	return s.mon.ResetSpanAndAlloc(span, newSize, s.Ctx())
}

// ResizeItem interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) ResizeItem(span *mon.AllocationSpan, oldSize, newSize int64) error {
	return s.mon.ResizeItem(span, oldSize, newSize, s.Ctx())
}

// ExtendSpan interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) ExtendSpan(span *mon.AllocationSpan, extraSize int64) error {
	return s.mon.ExtendSpan(span, extraSize, s.Ctx())
}
