// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package must provides a convenient and performant API for runtime assertions,
// similar to Testify. It is intended to encourage liberal use of assertions.
//
// Runtime assertions are great: they build confidence in invariants, plug gaps
// in test coverage, and run across all CRDB environments and workloads where
// they can detect bugs even in exceedingly rare corner cases.
//
// Assertions are used to check and enforce invariants, not for general error
// handling. As a rule of thumb, only use an assertion for something that you
// never expect to fail, ever. Some examples:
//
//   - Good assertion: a range's start key must be before its end key. This must
//     always be true, so this should be an assertion.
//
//   - Bad assertion: writing to a file must succeed. We expect this to fail
//     sometimes with e.g. faulty disks, so this should be a (possibly fatal)
//     error, not an assertion.
//
//   - Worse assertion: users must enter a valid SQL query. We expect users to
//     get this wrong all the time, so this should return an error.
//
// In non-release builds (including roachprod and roachtest clusters), assertion
// failures kill the process with a stack trace to ensure failures are noticed.
//
// In release builds, assertion failures do not kill the process, since this is
// often excessive -- for example, an assertion failure during RPC request
// processing should fail the RPC request, not kill the process. Instead, an
// assertion error is logged with a stack trace, reported to Sentry (if
// enabled), and returned to the caller. The caller should process the error as
// appropriate, e.g. return it up the stack or ignore it.
//
// In rare cases, it may be appropriate to always fatal or panic, regardless of
// build type, typically when it is unsafe to keep the process running. The
// helpers FatalOn() and PanicOn() can be used for this.
//
// Additionally, fatal assertions can be enabled or disabled via the
// COCKROACH_FATAL_ASSERTIONS environment variable, regardless of build type.
//
// Because assertion failures return an error, they must be explicitly handled,
// or the errcheck linter will complain. At the very least, the error must be
// ignored (if it is safe) with e.g. _ = must.True(ctx, foo, "bar"). This is
// intentional: we don't want to fatal in release builds, so the caller must
// consider how to handle the error there.
//
// This can also lead to non-fatal assertion failures being logged or reported
// in two places: at the assertion failure site, and where the original caller
// fails when it receives the error. This is also intentional, since it provides
// additional details about the caller, who is often an RPC client on a
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
//	  if err := must.Expensive(func() error {
//	    computedStats, err := store.ComputeStats(r.store.Engine, desc.StartKey, desc.EndKey, now)
//			if err != nil {
//				return err
//			}
//	    return must.Equal(ctx, r.mu.state.Stats, computedStats, "MVCC stats diverged")
//	  }); err != nil {
//			return err
//		}
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
package must

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/constraints"
)

var (
	// FatalAssertions will exit the process with a fatal error on assertion
	// failures. It defaults to true in non-release builds. Otherwise, an
	// assertion error is logged and returned.
	FatalAssertions = envutil.EnvOrDefaultBool("COCKROACH_FATAL_ASSERTIONS", !build.IsRelease())

	// MaybeSendReport is injected by package logcrash, and reports errors to
	// Sentry if enabled.
	MaybeSendReport func(ctx context.Context, err error)

	// OnFail can be injected in tests.
	OnFail func(ctx context.Context, err error)
)

// Fail triggers an assertion failure. In non-release builds, it fatals with a
// stack trace. In release builds, if returns an assertion error, logs it with a
// stack trace, and reports it to Sentry (if enabled).
//
// gcassert:inline
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
	} else if FatalAssertions {
		log.FatalfDepth(ctx, depth, "%+v", err)
	} else {
		log.ErrorfDepth(ctx, depth, "%+v", err)
		MaybeSendReport(ctx, err)
	}
	return err
}

// FatalOn unconditionally promotes an assertion failure to a fatal error, even
// in release builds or when fatal assertions are disabled. This should only be
// used when it is unsafe to keep the process running.
//
// gcassert:inline
func FatalOn(ctx context.Context, err error) {
	if err != nil {
		log.FatalfDepth(ctx, 1, "%+v", err)
	}
}

// PanicOn promotes a non-fatal assertion failure to a panic (typically in
// release builds). This is mostly for use in SQL APIs where errors are
// propagated as panics.
//
// gcassert:inline
func PanicOn(err error) {
	if err != nil {
		panic(fmt.Sprintf("%+v", err))
	}
}

// Expensive is used for assertions that must be compiled out of regular
// release/dev/test builds for performance reasons. They're enabled in race
// builds. The function should use assertions as normal and return failures.
//
// gcassert:inline
func Expensive(f func() error) error {
	// TODO(erikgrinaker): consider gating these on a different tag.
	if util.RaceEnabled {
		return f()
	}
	return nil
}

// True requires v to be true. Otherwise, fatals in dev builds or errors
// in release builds (by default).
//
// gcassert:inline
func True(ctx context.Context, v bool, format string, args ...interface{}) error {
	if v {
		return nil
	}
	return failDepth(ctx, 1, format, args...)
}

// False requires v to be false. Otherwise, fatals in dev builds or errors
// in release builds (by default).
//
// gcassert:inline
func False(ctx context.Context, v bool, format string, args ...interface{}) error {
	if !v {
		return nil
	}
	return failDepth(ctx, 1, format, args...)
}

// Equal requires a and b to be equal. Otherwise, fatals in dev builds or errors
// in release builds (by default).
//
// gcassert:inline
func Equal[T comparable](ctx context.Context, a, b T, format string, args ...interface{}) error {
	// TODO(erikgrinaker): Consider erroring out on pointers, if possible. It's
	// usually not what one wants, and can be a footgun.
	if a == b {
		return nil
	}
	// TODO(erikgrinaker): Consider printing a diff, or otherwise improve the
	// formatting, for large values like entire structs.
	format += ": %#v != %#v"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// NotEqual requires a and b not to be equal. Otherwise, fatals in dev builds or
// errors in release builds (by default).
//
// gcassert:inline
func NotEqual[T comparable](ctx context.Context, a, b T, format string, args ...interface{}) error {
	if a != b {
		return nil
	}
	format += ": %#v == %#v"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// Greater requires a > b. Otherwise, fatals in dev builds or errors in release
// builds (by default).
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

// GreaterOrEqual requires a >= b. Otherwise, fatals in dev builds or errors in
// release builds (by default).
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

// Less requires a < b. Otherwise, fatals in dev builds or errors in release
// builds (by default).
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

// LessOrEqual requires a <= b. Otherwise, fatals in dev builds or errors in
// release builds (by default).
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

// EqualBytes requires two byte slices or strings to be equal. Otherwise, fatals
// in dev builds or errors in release builds (by default).
//
// gcassert:inline
func EqualBytes[T ~[]byte | ~string](
	ctx context.Context, a, b T, format string, args ...interface{},
) error {
	if string(a) == string(b) {
		return nil
	}
	format += ": %q != %q"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// NotEqualBytes requires two byte slices or strings not to be equal. Otherwise,
// fatals in dev builds or errors in release builds (by default).
//
// gcassert:inline
func NotEqualBytes[T ~[]byte | ~string](
	ctx context.Context, a, b T, format string, args ...interface{},
) error {
	if string(a) != string(b) {
		return nil
	}
	format += ": %q == %q"
	return failDepth(ctx, 1, format, append(args, a, b)...)
}

// PrefixBytes requires a byte slice or string to have a given prefix.
// Otherwise, fatals in dev builds or errors in release builds (by default).
//
// gcassert:inline
func PrefixBytes[T ~[]byte | ~string](
	ctx context.Context, v, prefix T, format string, args ...interface{},
) error {
	if len(v) >= len(prefix) && string(v[:len(prefix)]) == string(prefix) {
		return nil
	}
	format += ": %q does not have prefix %q"
	return failDepth(ctx, 1, format, append(args, v, prefix)...)
}

// NotPrefixBytes requires a byte slice or string to not have a given prefix.
// Otherwise, fatals in dev builds or errors in release builds (by default).
//
// gcassert:inline
func NotPrefixBytes[T ~[]byte | ~string](
	ctx context.Context, v, prefix T, format string, args ...interface{},
) error {
	if len(v) < len(prefix) || string(v[:len(prefix)]) != string(prefix) {
		return nil
	}
	format += ": %q has prefix %q"
	return failDepth(ctx, 1, format, append(args, v, prefix)...)
}

// Len requires a slice to have the specified length. Otherwise, fatals in dev
// builds or errors in release builds (by default).
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
	return failDepth(ctx, 1, format, append(args, l, length)...)
}

// Contains requires the slice to contain the given element. Otherwise, fatals
// in dev builds or errors in release builds (by default).
//
// gcassert:inline
func Contains[V comparable, T ~[]V](
	ctx context.Context, slice T, value V, format string, args ...interface{},
) error {
	for _, v := range slice {
		if v == value {
			return nil
		}
	}
	format += ": %v not in %v"
	return failDepth(ctx, 1, format, append(args, value, slice)...)
}

// NotContains requires the slice to not contain the given element. Otherwise,
// fatals in dev builds or errors in release builds (by default).
//
// gcassert:inline
func NotContains[V comparable, T ~[]V](
	ctx context.Context, slice T, value V, format string, args ...interface{},
) error {
	for _, v := range slice {
		if v == value {
			format += ": %v is in %v"
			return failDepth(ctx, 1, format, append(args, value, slice)...)
		}
	}
	return nil
}

// Empty requires an empty slice. Otherwise, fatals in dev builds or errors in
// release builds (by default).
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

// NotEmpty requires a non-empty slice. Otherwise, fatals in dev builds or
// errors in release builds (by default).
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

// Nil requires v to be nil. Otherwise, fatals in dev builds or errors in
// release builds (by default).
//
// TODO(erikgrinaker): Should handle interfaces too, if possible with generics,
// but beware typed and untyped nils.
//
// gcassert:inline
func Nil[T any](ctx context.Context, v *T, format string, args ...interface{}) error {
	if v == nil {
		return nil
	}
	format += ": expected nil, got %#v"
	return failDepth(ctx, 1, format, append(args, v)...)
}

// NotNil requires v not to be nil. Otherwise, fatals in dev builds or errors in
// release builds (by default).
//
// gcassert:inline
func NotNil[T any](ctx context.Context, v *T, format string, args ...interface{}) error {
	if v != nil {
		return nil
	}
	format += ": value is nil"
	return failDepth(ctx, 1, format, args...)
}

// Zero requires v to be zero-valued for its type. Otherwise, fatals in dev
// builds or errors in release builds (by default).
//
// gcassert:inline
func Zero[T comparable](ctx context.Context, v T, format string, args ...interface{}) error {
	var zero T
	if v == zero {
		return nil
	}
	format += ": expected zero value, got %#v"
	return failDepth(ctx, 1, format, append(args, v)...)
}

// NotZero requires v not to be zero-valued for its type. Otherwise, fatals in
// dev builds or errors in release builds (by default).
//
// gcassert:inline
func NotZero[T comparable](ctx context.Context, v T, format string, args ...interface{}) error {
	var zero T
	if v != zero {
		return nil
	}
	format += ": %#v is zero-valued"
	return failDepth(ctx, 1, format, append(args, v)...)
}

// Same requires a and b to point to the same object (i.e. memory location).
// Otherwise, fatals in dev builds or errors in release builds (by default).
//
// gcassert:inline
func Same[T any](ctx context.Context, a, b *T, format string, args ...interface{}) error {
	if a == b {
		return nil
	}
	format += ": %p != %p\n%p is %#v\n%p is %#v'"
	return failDepth(ctx, 1, format, append(args, a, b, a, a, b, b)...)
}

// NotSame requires a and b to not point to the same object (i.e. memory
// location). Otherwise, fatals in dev builds or errors in release builds (by
// default).
//
// gcassert:inline
func NotSame[T any](ctx context.Context, a, b *T, format string, args ...interface{}) error {
	if a != b {
		return nil
	}
	format += ": %p == %p, %#v"
	return failDepth(ctx, 1, format, append(args, a, b, a)...)
}

// Error requires a non-nil error. Otherwise, fatals in dev builds or errors in
// release builds (by default).
//
// gcassert:inline
func Error(ctx context.Context, err error, format string, args ...interface{}) error {
	if err != nil {
		return nil // nolint:returnerrcheck
	}
	format += ": expected error, got nil"
	return failDepth(ctx, 1, format, args...)
}

// NoError requires a nil error. Otherwise, fatals in dev builds or errors in
// release builds (by default).
//
// gcassert:inline
func NoError(ctx context.Context, err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	format += ": error %v"
	return failDepth(ctx, 1, format, append(args, err)...)
}
