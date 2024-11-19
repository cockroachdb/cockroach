// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package startup provides functionality for retrying kv operations on server
// startup.
//
// Server startup is now heavily dependent on kv for retrieving various
// metadata. It is possible for operations to fail with ambiguous error in case
// of timeouts. This problem is exacerbated by introduction of circuit breakers.
// Circuit breaker will fail fast if previous access to the replica failed
// and will try to do probing and reset in the background.
// Because of this, startup operations could fail immediately if consensus is
// lost on a range thus preventing node from restarting. If the node itself is
// needed to reestablish range consensus then the node would not be able to
// rejoin the cluster at all after circuit breaker is activated on that range
// unless circuit breaker is reset by restarting all nodes.
//
// To address this problem, all kv operations that are part of the start-up
// sequence  must be retried if they encounter kvpb.ReplicaUnavailableError.
// Those errors could be returned from both kv and sql queries.
//
// This package provides functionality to wrap functions in retry loops as well
// as set of assertions to verify that all operations are doing retries during
// startup.
//
// This behaviour is achieved by checking call stack in kv operations and
// queries in internal sql executor. Latter is needed because we can't assert kv
// operations run by executor in separate go routines.
// Assertions expect call context be tagged with startupRetryKey. If they detect
// that call is on startup path and don't have retry tag, server will crash.
// Provided retry functions tag context that is passed to functions being
// retried.
// If test has a test only code path (e.g. avoid background operations for test)
// it is possible to disable retry check by using WithoutChecks decorator for
// context.
//
// Retry tracking is only done when build is run with util.RaceEnabled.
//
// Separate Begin function should be called at the beginning of the startup
// and returned cleanup function should be called at the end. This ensures that
// startup stack is correctly detected and that test clusters are not
// excessively penalized by assertions when startup is complete.
//
// If your test fails because startup assertion failed, then there's a new code
// path run during server startup which is not correctly wrapped in retry loop
// and it could cause node startup failures. You need to identify what is called
// from Server.PreStart and add retry accordingly.
package startup

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/petermattis/goid"
)

// startupRetryKey context key used by assertions to track that code is running
// inside startup retry loop.
type startupRetryKey struct{}

// startupRetryOpts is retry config for startup operations.
var startupRetryOpts = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     3 * time.Second,
	Multiplier:     2,
}

// runningStartup is a counter showing number of concurrently running server
// startups. If it is zero, then startups are finished and we should not try
// to run startup retry checks.
// This counter only makes sense and used when running TestCluster and is an
// optimization to stop doing any potentially expensive assertions and obtaining
// startupGoroutineIDs lock once servers are running.
// Mind that this is a best effort tracking and assertions themselves should
// not give false positives/negatives regardless of this counter value.
var runningStartup atomic.Int32

type goroutineIDs struct {
	syncutil.RWMutex
	ids map[int64]bool
}

// startupGoroutineIDs contains id's of goroutines on which startup is running.
// We could have more than a single one in integration tests where TestCluster
// uses parallel startup.
var startupGoroutineIDs = goroutineIDs{
	ids: make(map[int64]bool),
}

// WithoutChecks adds startup tag to context to allow kv and sql executor access
// from startup path without wrapping methods in RunIdempotentWithRetry*.
func WithoutChecks(ctx context.Context) context.Context {
	return context.WithValue(ctx, startupRetryKey{}, "bypass retry check")
}

// RunIdempotentWithRetry synchronously runs a function with retry of circuit
// breaker errors on startup. Since we retry ambiguous errors unconditionally,
// operations should be idempotent by nature. If this is not possible, then
// retry should be performed explicitly while using WithoutChecks context
// to suppress safety mechanisms.
func RunIdempotentWithRetry(
	ctx context.Context, quiesce <-chan struct{}, opName string, f func(ctx context.Context) error,
) error {
	_, err := RunIdempotentWithRetryEx(ctx, quiesce, opName, func(ctx context.Context) (any, error) {
		return nil, f(ctx)
	})
	return err
}

// RunIdempotentWithRetryEx run function returning value with startup retry.
// See RunIdempotentWithRetry for important details.
func RunIdempotentWithRetryEx[T any](
	ctx context.Context,
	quiesce <-chan struct{},
	opName string,
	f func(ctx context.Context) (T, error),
) (T, error) {
	ctx = context.WithValue(ctx, startupRetryKey{}, "in retry")
	every := log.Every(5 * time.Second)
	// Retry failures indefinitely until context is cancelled.
	var result T
	var err error
	retryOpts := startupRetryOpts
	retryOpts.Closer = quiesce
	for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
		result, err = f(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil || !IsRetryableReplicaError(err) {
			break
		}
		if every.ShouldLog() {
			log.Infof(ctx, "failed %s during node startup, retrying %s", opName, err)
		}
	}
	return result, err
}

// Begin ensures that startup detection is correctly identifying startup stack
// and initializing internal assertions for tests. It must be called first
// on server startup before any calls to kv or sql are made.
// Returned function must be called when startup function is complete.
func Begin(ctx context.Context) func() {
	if !util.RaceEnabled {
		return func() {}
	}

	// Ensure we don't try to call add twice by mistake before inserting our ID.
	startupID := goid.Get()
	startupGoroutineIDs.Lock()
	defer startupGoroutineIDs.Unlock()
	if _, ok := startupGoroutineIDs.ids[startupID]; ok {
		log.Fatal(ctx, "startup.Begin() is called twice")
	}
	startupGoroutineIDs.ids[startupID] = true

	// Maintain counter as atomic to avoid doing full locks on every send
	// operation.
	runningStartup.Add(1)
	return func() {
		startupGoroutineIDs.Lock()
		delete(startupGoroutineIDs.ids, startupID)
		startupGoroutineIDs.Unlock()
		runningStartup.Add(-1)
	}
}

// AssertStartupRetry is called by dist sender and internal sql executor to
// ensure that server startup is always performing retry of kv operations.
func AssertStartupRetry(ctx context.Context) {
	if !util.RaceEnabled {
		return
	}
	if runningStartup.Load() < 1 {
		return
	}

	rv := ctx.Value(startupRetryKey{})
	if rv == nil && inStartup() {
		log.Fatal(ctx, "startup query called outside of startup retry loop. See util/startup/retry.go docs")
	}
}

// inStartup returns true if currently running go routine called Begin before.
func inStartup() bool {
	currentID := goid.Get()
	startupGoroutineIDs.RLock()
	defer startupGoroutineIDs.RUnlock()
	return startupGoroutineIDs.ids[currentID]
}

// IsRetryableReplicaError returns true for replica availability errors.
func IsRetryableReplicaError(err error) bool {
	return errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil)) ||
		errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)) ||
		// See https://github.com/cockroachdb/cockroach/issues/104016#issuecomment-1569719273.
		pgerror.GetPGCode(err) == pgcode.RangeUnavailable
}
