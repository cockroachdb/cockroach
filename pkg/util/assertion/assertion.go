// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package assertion provides a canonical API for runtime assertions, intended
// to encourage liberal use of assertions in a way that's safe and performant.
// These help build confidence in invariants and assumptions, plug gaps in test
// coverage, and run across all CRDB environments and workloads where they can
// detect bugs even in exceedingly rare corner cases.
//
// Typical usage:
//
//	if foo != bar {
//	    return assertion.Failed(ctx, "oh no: %v != %v", foo, bar)
//	}
//
// (the API is not e.g. Assert(foo == bar, "oh no: %v != %v", foo, bar) since
// this incurs unconditional runtime overhead for the format interface boxing)
//
// In non-release builds (including roachprod and roachtest clusters), assertion
// failures kill the process with a stack trace to ensure failures are noticed.
//
// In release builds, assertion failures are not fatal, since this is often
// excessive and causes unnecessary disruption and outages. Instead, an error is
// logged (with stack trace), reported to Sentry, and returned to the caller.
// The caller must explicitly handle the error (enforced by the errcheck
// linter), and should typically propagate it, but can also choose to either
// ignore or recover from the error (when safe), or unconditionally kill the
// process if necessary even in release builds.
//
// This can also be controlled explicitly via COCKROACH_FATAL_ASSERTIONS.
//
// Assertions are used to check and enforce internal invariants, not for general
// error handling or fatal errors. As a rule of thumb, only use an assertion for
// something we expect will never ever fail. Some examples:
//
//   - Good assertion: a Raft range's start key must be before its end key. This
//     must always be true, so this should be an assertion.
//
//   - Bad assertion: writing to a file must succeed. We expect this to fail
//     sometimes, e.g. with faulty disks, so this should be a (possibly fatal)
//     error, not an assertion.
//
//   - Worse assertion: users must enter a valid SQL query. We expect users to
//     get this wrong all the time, so this should return an error. In general,
//     assertions should never be used to validate external inputs.
//
// Sometimes, it may be appropriate to panic or fatal on assertion failures even
// in release builds. Panics are often used to propagate errors in SQL code.
// Fatals may be necessary when it is unsafe to keep the node running. The
// helpers Panic() and Fatal() can be used for these cases, but only sparingly.
//
// Some assertions may be too expensive to always check. These can be gated on
// ExpensiveEnabled, which is set to true in special test builds (currently race
// builds), and will otherwise compile the assertion out entirely.
//
// USAGE EXAMPLES
// ==============
//
// Example: ignoring an assertion failure. Here, RPC batch processing should
// never return both a Go error and a response error. In release builds, we're
// ok with just ignoring this and using the Go error.
//
//	func (n *Node) Batch(ctx context.Context, req *kvpb.BatchRequest) *kvpb.BatchResponse {
//	  br, err := n.batchInternal(ctx, req)
//	  if err != nil {
//	    if br.Error != nil {
//	      _ = assertion.Failed(ctx, "returned both br.Err=%v and err=%v", br.Err, err)
//	    }
//	    br.Error = err // runs in release builds
//	  }
//	  return br
//	}
//
// Example: returning early on assertion failure. Here, we guard against
// double-stopping a processor. In release builds, we can ignore the error and
// return early, since the processor has already been stopped.
//
//	func (p *Processor) Stop(ctx context.Context) {
//	  if p.stopped {
//	    _ = assertion.Failed(ctx, "already stopped")
//	    return // runs in release builds
//	  }
//	  p.stopped = true
//	}
//
// Example: recovering from an assertion failure. Here, we assert that a byte
// budget isn't closed with outstanding allocations. In release builds, we
// simply release the outstanding bytes and continue closing.
//
//	func (m *BytesMonitor) Close(ctx context.Context) {
//	  m.mu.Lock()
//	  defer m.mu.Unlock()
//	  if m.mu.curAllocated != 0 {
//	    _ = assertion.Failed(ctx, "unexpected %d leftover bytes", m.mu.curAllocated)
//	    m.releaseBytesLocked(ctx, m.mu.curAllocated) // runs in release builds
//	  }
//	  mm.releaseBudget(ctx)
//	}
//
// Example: returning an error on assertion failure. Here, we guard against
// unknown transaction statuses during EndTxn. In release builds, we return an
// error to the RPC client.
//
//	func EndTxn(
//	  ctx context.Context, rw storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
//	) (result.Result, error) {
//	  switch reply.Txn.Status {
//	  case roachpb.COMMITTED: // ...
//	  case roachpb.ABORTED:   // ...
//	  case roachpb.PENDING:   // ...
//	  case roachpb.STAGING:   // ...
//	  default:
//	    err := assertion.Failed(ctx, "invalid txn status %s", reply.Txn.Status)
//	    return result.Result{}, err // runs in release builds
//	  }
//	}
//
// Example: expensive assertions. Here, we assert that a range's MVCC stats are
// accurate after every write, verifying that incremental updates done by writes
// are correct. This is too expensive to do even in non-release builds, so we
// gate the assertion on ExpensiveEnabled which is only enabled in special test
// builds.
//
//	func (b *replicaAppBatch) ApplyToStateMachine(ctx context.Context) error {
//	  // ...
//	  if assertion.ExpensiveEnabled {
//	    // runs in certain test builds
//	    stats, err := storage.ComputeStats(b.r.store.Engine, desc.StartKey, desc.EndKey, now)
//	    if err != nil {
//	      return err
//	    }
//	    if stats != *b.state.Stats {
//	      return assertion.Failed(ctx, "incorrect MVCC stats, %v != %v", stats, b.state.Stats)
//	    }
//	  }
//	  // ...
//	}
//
// Example: fatal errors, which should not be assertions. Here, Pebble detects
// file corruption. This is not an assertion, but rather normal error handling,
// so we fatal via log.Fatalf() even in release builds.
//
//	if err := iter.Close(); err == pebble.ErrCorruption {
//	  log.Fatalf(ctx, "%v", err)
//	}
package assertion

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var (
	// shouldFatal exits the process with a fatal error on assertion failures. It
	// defaults to true in non-release builds.
	shouldFatal = envutil.EnvOrDefaultBool("COCKROACH_FATAL_ASSERTIONS", !build.IsRelease())

	// MaybeSendReport is injected by package logcrash, for Sentry reporting.
	MaybeSendReport func(ctx context.Context, err error)
)

// ExpensiveEnabled is true if expensive assertions are enabled.
//
// TODO(erikgrinaker): This should use a separate/different build tag, but
// for now we gate them on race which has de-facto been used for this. See:
// https://github.com/cockroachdb/cockroach/issues/107425
const ExpensiveEnabled = util.RaceEnabled

// Failed reports an assertion failure. In non-release builds, it fatals with a
// stack trace. In release builds, if returns an assertion error, logs it with a
// stack trace, and reports it to Sentry (if enabled).
func Failed(ctx context.Context, format string, args ...interface{}) error {
	return failedDepth(ctx, 1, format, args...)
}

// failedDepth is like Failed, but removes the given number of stack frames in
// the stack trace and log message source code location.
func failedDepth(ctx context.Context, depth int, format string, args ...interface{}) error {
	depth += 1
	err := errors.AssertionFailedWithDepthf(depth, format, args...)
	if shouldFatal {
		log.FatalfDepth(ctx, depth, "%+v", err)
	} else {
		log.ErrorfDepth(ctx, depth, "%+v", err)
		if MaybeSendReport != nil { // can be nil in tests
			MaybeSendReport(ctx, err)
		}
	}
	return err
}

// Fatal unconditionally fatals the process, even in release builds or when
// fatal assertions are disabled. This should only be used when it is unsafe to
// keep the process running. Prefer Failed() when possible.
func Fatal(ctx context.Context, format string, args ...interface{}) {
	// We don't call through to failedDepth(), to avoid logging and notifying the
	// error twice.
	err := errors.AssertionFailedWithDepthf(1, format, args...)
	log.FatalfDepth(ctx, 1, "%+v", err)
}

// Panic unconditionally panics with an assertion error, even in release builds.
// This is primarily for use in SQL code where errors are sometimes propagated
// as panics. Prefer Failed() when possible.
func Panic(ctx context.Context, format string, args ...interface{}) {
	panic(failedDepth(ctx, 1, format, args...))
}
