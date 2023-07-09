// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package must provides a convenient API for runtime assertions.
//
// Runtime assertions are great: they build confidence in assumptions and
// invariants, plug gaps in test coverage, and run in every single CRDB instance
// across all environments and workloads. They can detect bugs in exceedingly
// rare corner cases, and help with debugging incidents.
//
// We should liberally sprinkle assertions everywhere. This package makes that
// easy. It provides familiar Testify-style assertions, similar to those used in
// tests, which are cheap enough to not impact performance in the common case.
//
// In non-release builds (including roachprod and roachtest clusters), assertion
// failures kill the process (with a stack trace) to ensure they are noticed.
//
// In release builds, assertion failures do not kill the process, since this is
// often excessive -- for example, an assertion failure during RPC request
// processing should fail the RPC request, not kill the process. Instead, an
// assertion error is logged with a stack trace, reported to Sentry (if
// enabled), and returned to the caller. The caller should process the error as
// appropriate, e.g. return it up the stack or ignore it.
//
// Because assertion failures return an error, they must be explicitly handled,
// or the errcheck linter will complain. At the very least, the error must be
// ignored with e.g. _ = must.True(ctx, foo, "bar"). This is intentional: we
// don't want to fatal in release builds, so the caller must consider how to
// handle the error there.
//
// This can also lead to assertion failures being logged/reported in two places
// in release builds: at the assertion failure site, and where the original
// caller fails when it receives the error. This is also intentional, since it
// provides additional details about the caller, who is often an RPC client on a
// different node (thus stack traces can be insufficient by themselves).
//
// Some assertions may be too expensive to always run. For example, we may want
// to recompute a range's MVCC stats after every write command, and assert that
// they equal the incremental stats computed by the command. This can be done
// via must.Expensive(), which only runs the given assertions in special test
// builds (currently race builds), and compiles them out in other builds.
//
// Below are several usage examples.
//
// Example: guarding against long log lines. In release builds, the assertion
// failure can be ignored, since it does not affect the subsequent logic (we can
// still log the line even if it's long).
//
//	func Log(ctx context.Context, line string) {
//	  _ = must.LessOrEqual(ctx, len(line), 1024, "log line too long: %q", line)
//	  log.Infof(ctx, line) // runs in release builds
//	}
//
// Example: double-stopping a component. In release builds, we can simply ignore
// the failure and return early, since it has already been stopped.
//
//	func (p *Processor) Stop(ctx context.Context) {
//	  if err := must.False(ctx, p.stopped, "already stopped"); err != nil {
//	    _ = err
//	    return // runs in release builds
//	  }
//	  p.stopped = true
//	  // ...
//	}
//
// Example: missing lease during a range split. Return an assertion error to the
// caller which fails the split, since we have an error return path.
//
//	func splitTrigger(
//	  ctx context.Context, rec EvalContext, batch storage.Batch, split *roachpb.SplitTrigger,
//	) (result.Result, error) {
//	  leftLease, err := MakeStateLoader(rec).LoadLease(ctx, batch)
//	  if err != nil {
//	    return result.Result{}, err
//	  }
//	  if err := must.NotZero(ctx, leftLease, "LHS of split has no lease"); err != nil {
//	    return result.Result{}, err // runs in release builds
//	  }
//	  // ...
//	}
//
// Example: unknown txn status enum value during EndTxn. We call must.Fail
// directly since we've already checked the condition, and return the error to
// the caller in release builds.
//
//	func EndTxn(
//	  ctx context.Context, rw storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
//	) (result.Result, error) {
//	  // ...
//	  switch reply.Txn.Status {
//	  case roachpb.COMMITTED: // ...
//	  case roachpb.ABORTED:   // ...
//	  case roachpb.PENDING:    // ...
//	  case roachpb.STAGING:    // ...
//	  default:
//	    err := must.Fail(ctx, "invalid txn status %s", reply.Txn.Status)
//	    return result.Result{}, err // runs in release builds
//	  }
//	  // ...
//	}
//
// Example: verify range MVCC stats are accurate after every write. This is too
// expensive to do even in non-release builds, but we may want to check this in
// dedicated test builds.
//
//	func (b *replicaAppBatch) ApplyToStateMachine(ctx context.Context) error {
//	  *r.mu.state.Stats = *b.state.Stats
//	  must.Expensive(func() {
//	    computedStats, err := store.ComputeStats(r.store.Engine, desc.StartKey, desc.EndKey, now)
//	    _ = must.NoError(t, err)
//	    _ = must.Equal(ctx, r.mu.state.Stats, computedStats, "MVCC stats diverged")
//	  })
//	  // ...
//	}
//
// Example: Pebble returns a corruption error, so we should kill the process.
// This is not an assertion, but rather legitimate error handling, so we fatal
// via log.Fatalf() even in release builds.
//
//	if err := iter.Close(); err == pebble.ErrCorruption {
//	  log.Fatalf(ctx, "%v", err)
//	}
//
// It may sometimes (very rarely) be appropriate to fatal or panic on assertion
// failures even in release builds. In these cases, the helpers `FatalOn()` and
// `PanicOn()` may be used.
package must

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/constraints"
)

// MaybeSendReport is injected by package logcrash, and reports errors to Sentry
// if enabled.
var MaybeSendReport func(ctx context.Context, err error)

// OnFail can be injected in tests.
var OnFail func(ctx context.Context, err error)

// Fail triggers an assertion failure. In non-release builds, it fatals with a
// stack trace. In release builds, if returns an assertion error, logs it with a
// stack trace, and reports it to Sentry (if enabled).
func Fail(ctx context.Context, format string, args ...interface{}) error {
	return failDepth(ctx, 1, format, args...)
}

// failDepth is like Fail, but removes the given number of stack frames in the
// stack trace and the source code origin of the log message.
func failDepth(ctx context.Context, depth int, format string, args ...interface{}) error {
	depth += 1
	err := errors.AssertionFailedWithDepthf(depth, format, args...)
	if OnFail != nil {
		OnFail(ctx, err)
	} else if !build.IsRelease() {
		log.FatalfDepth(ctx, depth, "%+v", err)
	} else {
		log.ErrorfDepth(ctx, depth, "%+v", err)
		MaybeSendReport(ctx, err)
	}
	return err
}

// FatalOn promotes an assertion failure to a panic in release builds. In
// non-release builds, the original assertion failure fatals as usual. This
// should be used very rarely, only when it is appropriate to fatal the node
// even in release builds because it is provably unsafe to continue.
//
// gcassert:inline
func FatalOn(ctx context.Context, err error) {
	if err != nil {
		log.Fatalf(ctx, "%+v", err)
	}
}

// PanicOn promotes an assertion failure to a panic in release builds. In
// non-release builds, the original assertion failure fatals as usual. This is
// mostly for use in SQL APIs where errors are propagated as panics. It should
// otherwise not be used.
//
// gcassert:inline
func PanicOn(err error) {
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
}

// Expensive is used for assertions that must be disabled in regular
// release/dev/test builds for performance reasons. They're enabled in invariant
// or race builds. The given function should use assertions as usual, which will
// always fatal since Expensive is never enabled in release builds.
func Expensive(f func()) {
	if buildutil.Invariants {
		f()
	}
}

// True requires v to be true. Otherwise, fatals in dev/test builds, or returns
// an assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func True(ctx context.Context, v bool, format string, args ...interface{}) error {
	if v {
		return nil
	}
	return failDepth(ctx, 1, format, args...)
}

// False requires v to be false. Otherwise, fatals in dev/test builds, or
// returns an assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func False(ctx context.Context, v bool, format string, args ...interface{}) error {
	if !v {
		return nil
	}
	return failDepth(ctx, 1, format, args...)
}

// Equal requires a and b to be equal. Otherwise, fatals in dev/test builds, or
// returns an assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func Equal[T comparable](ctx context.Context, a, b T, format string, args ...interface{}) error {
	// TODO(erikgrinaker): Consider erroring out on pointers, if possible. It's
	// usually not what one wants, and can be a footgun.
	if a == b {
		return nil
	}
	format += ": %#v != %#v"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// NotEqual requires a and b not to be equal. Otherwise, fatals in dev/test
// builds, or returns an assertion error that's also logged and reported in
// release builds.
//
// gcassert:inline
func NotEqual[T comparable](ctx context.Context, a, b T, format string, args ...interface{}) error {
	if a != b {
		return nil
	}
	format += ": %#v == %#v"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// Greater requires a > b. Otherwise, fatals in dev/test builds, or returns an
// assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func Greater[T constraints.Ordered](
	ctx context.Context, a, b T, format string, args ...interface{},
) error {
	if a > b {
		return nil
	}
	format += ": %v <= %v"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// GreaterOrEqual requires a >= b. Otherwise, fatals in dev/test builds, or
// returns an assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func GreaterOrEqual[T constraints.Ordered](
	ctx context.Context, a, b T, format string, args ...interface{},
) error {
	if a >= b {
		return nil
	}
	format += ": %v < %v"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// Less requires a < b. Otherwise, fatals in dev/test builds, or returns an
// assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func Less[T constraints.Ordered](
	ctx context.Context, a, b T, format string, args ...interface{},
) error {
	if a < b {
		return nil
	}
	format += ": %v >= %v"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// LessOrEqual requires a <= b. Otherwise, fatals in dev/test builds, or returns
// an assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func LessOrEqual[T constraints.Ordered](
	ctx context.Context, a, b T, format string, args ...interface{},
) error {
	if a <= b {
		return nil
	}
	format += ": %v > %v"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// Error requires a non-nil error. Otherwise, fatals in dev/test builds, or
// returns an assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func Error(ctx context.Context, err error, format string, args ...interface{}) error {
	if err != nil {
		return nil // nolint:returnerrcheck
	}
	format += ": expected error but got none"
	return failDepth(ctx, 1, format, args...)
}

// NoError requires a nil error. Otherwise, fatals in dev/test builds, or
// returns an assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func NoError(ctx context.Context, err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	format += ": error %T %v"
	return failDepth(ctx, 1, format, append(args, err, err)...)
}

// Len requires a slice to have the specified length. Otherwise, fatals in
// dev/test builds, or returns an assertion error that's also logged and
// reported in release builds.
//
// TODO(erikgrinaker): This should handle maps and strings too, if possible.
//
// gcassert:inline
func Len[V any, T ~[]V](
	ctx context.Context, c T, length int, format string, args ...interface{},
) error {
	l := len(c)
	if l == length {
		return nil
	}
	format += ": length %d != %d"
	return failDepth(ctx, 1, format, append(args, l, length, c)...)
}

// Empty requires an empty slice. Otherwise, fatals in dev/test builds, or
// returns an assertion error that's also logged and reported in release builds.
//
// TODO(erikgrinaker): This should handle maps and strings too, if possible.
//
// gcassert:inline
func Empty[V any, T ~[]V](ctx context.Context, c T, format string, args ...interface{}) error {
	l := len(c)
	if l == 0 {
		return nil
	}
	format += ": length %d != 0"
	return failDepth(ctx, 1, format, append(args, l)...)
}

// NotEmpty requires a non-empty slice. Otherwise, fatals in dev/test builds, or
// returns an assertion error that's also logged and reported in release builds.
//
// TODO(erikgrinaker): This should handle maps and strings too, if possible.
//
// gcassert:inline
func NotEmpty[V any, T ~[]V](ctx context.Context, c T, format string, args ...interface{}) error {
	if len(c) > 0 {
		return nil
	}
	format += ": is empty"
	return failDepth(ctx, 1, format, args...)
}

// Nil requires v to be nil. Otherwise, fatals in dev/test builds, or returns an
// assertion error that's also logged and reported in release builds.
//
// TODO(erikgrinaker): Should handle interfaces too, if possible with generics,
// but beware typed and untyped nils.
//
// gcassert:inline
func Nil[T any](ctx context.Context, v *T, format string, args ...interface{}) error {
	if v == nil {
		return nil
	}
	format += ": %#v is not nil"
	return failDepth(ctx, 1, format, append(args, v)...)
}

// NotNil requires v not to be nil. Otherwise, fatals in dev/test builds, or
// returns an assertion error that's also logged and reported in release builds.
//
// gcassert:inline
func NotNil[T any](ctx context.Context, v *T, format string, args ...interface{}) error {
	if v != nil {
		return nil
	}
	format += ": %#v is nil"
	return failDepth(ctx, 1, format, append(args, v)...)
}

// Zero requires v to be zero-valued for its type. Otherwise, fatals in dev/test
// builds, or returns an assertion error that's also logged and reported in
// release builds.
//
// gcassert:inline
func Zero[T comparable](ctx context.Context, v T, format string, args ...interface{}) error {
	var zero T
	if v == zero {
		return nil
	}
	format += ": %#v is not zero-valued"
	return failDepth(ctx, 1, format, append(args, v)...)
}

// NotZero requires v not to be zero-valued for its type. Otherwise, fatals in
// dev/test builds, or returns an assertion error that's also logged and
// reported in release builds.
//
// gcassert:inline
func NotZero[T comparable](ctx context.Context, v T, format string, args ...interface{}) error {
	var zero T
	if v != zero {
		return nil
	}
	format += ": is zero-valued"
	return failDepth(ctx, 1, format, append(args, v)...)
}

// Same requires a and b to point to the same object (i.e. memory location).
// Otherwise, fatals in dev/test builds, or returns an assertion error that's
// also logged and reported in release builds.
//
// gcassert:inline
func Same[T any](ctx context.Context, a, b *T, format string, args ...interface{}) error {
	if a == b {
		return nil
	}
	format += ": %p != %p"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// NotSame requires a and b to not point to the same object (i.e. memory
// location). Otherwise, fatals in dev/test builds, or returns an assertion
// error that's also logged and reported in release builds.
//
// gcassert:inline
func NotSame[T any](ctx context.Context, a, b *T, format string, args ...interface{}) error {
	if a != b {
		return nil
	}
	format += ": %p == %p"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}
