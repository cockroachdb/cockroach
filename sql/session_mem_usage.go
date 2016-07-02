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
// session, and the logical timespan for tracking memory usage is
// also bound to the entire duration of the session.
//
// The "logical timespan" is the duration between the point in time where
// to "begin" monitoring (set counters to 0) and where to "end"
// monitoring (check that if counters != 0 then there was a leak, and
// report that in logs/errors).
//
// Alternatives to define the logical timespan were considered and
// rejected:
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
// Ideally we would want a "magic" timespan that extends automatically
// from the start of a transaction to the point where all related
// Results (ResultList items) have been sent back to the
// client. However with this definition and the current code there can
// be multiple such "magic" timespans alive simultaneously. This is
// because a client can start a new transaction before it reads the
// ResultList of a previous transaction, e.g. if issuing `BEGIN;
// SELECT; COMMIT; BEGIN; SELECT; COMMIT` in one pgwire message.
//
// A way forward to implement this "magic" timespan would be to
// fix/implement #7775 (stream results from Executor to pgwire) and
// take care that the corresponding new streaming/pipeline logic
// passes a transaction-bound context to the monitor throughout.

// OpenAccount interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) OpenAccount(acc *mon.MemoryAccount) {
	s.mon.OpenAccount(s.Ctx(), acc)
}

// OpenAndInitAccount interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) OpenAndInitAccount(acc *mon.MemoryAccount, init int64) error {
	return s.mon.OpenAndInitAccount(s.Ctx(), acc, init)
}

// GrowAccount interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) GrowAccount(acc *mon.MemoryAccount, extraSize int64) error {
	return s.mon.GrowAccount(s.Ctx(), acc, extraSize)
}

// CloseAccount interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) CloseAccount(acc *mon.MemoryAccount) {
	s.mon.CloseAccount(s.Ctx(), acc)
}

// ClearAccount interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) ClearAccount(acc *mon.MemoryAccount) {
	s.mon.ClearAccount(s.Ctx(), acc)
}

// ClearAccountAndAlloc interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) ClearAccountAndAlloc(acc *mon.MemoryAccount, newSize int64) error {
	return s.mon.ClearAccountAndAlloc(s.Ctx(), acc, newSize)
}

// ResizeItem interfaces between Session and mon.MemoryUsageMonitor.
func (s *Session) ResizeItem(acc *mon.MemoryAccount, oldSize, newSize int64) error {
	return s.mon.ResizeItem(s.Ctx(), acc, oldSize, newSize)
}
