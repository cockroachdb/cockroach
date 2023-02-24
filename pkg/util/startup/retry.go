// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package startup provides functionality for retrying kv operations on server
// startup.
//
// Server startup is now heavily dependent on kv for retrieving various
// metadata. With introduction of circuit breakers, it is now possible for
// operations to fail fast without waiting for raft consensus recovery, with
// actual recovery happening in the background.
// Because of this, startup operations could fail immediately if consensus is
// lost on a range. If the node itself is needed to reestablish range consensus
// then the node would not be able to rejoin the cluster after circuit breaker
// is activated on that range.
//
// To address this problem, all kv operations must be retried if they encounter
// kvpb.ReplicaUnavailableError. Those errors could be returned from both kv and
// sql queries.
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
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// startupRetryKey context key used by assertions to track that code is running
// inside startup retry loop.
type startupRetryKey struct{}

// startupRetryOpts is retry config for startup operations. When changing values
// ensure that retriesBeforeLogging is updated accordingly as to not spam logs
// before at least couple of seconds have passed.
var startupRetryOpts = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     3 * time.Second,
	Multiplier:     2,
}

// retriesBeforeLogging is number of retries startup loop does before it starts
// logging errors.
const retriesBeforeLogging = 6

// runningStartup is a counter showing number of concurrently running server
// startups. If it is zero, then startups are finished and we should not try
// to run startup retry checks.
// This counter only makes sense and used when running TestCluster and is an
// optimization to stop doing any potentially expensive assertions once servers
// are running.
// Mind that this is a best effort tracking and assertions themselves should
// not give false positives/negatives regardless of this counter value.
var runningStartup atomic.Int32

// WithoutChecks adds startup tag to context to allow kv and sql executor access
// from startup path without wrapping methods in RunWithRetry*.
func WithoutChecks(ctx context.Context) context.Context {
	return context.WithValue(ctx, startupRetryKey{}, "bypass retry check")
}

// RunWithRetry synchronously runs a function with retry of circuit
// breaker errors on startup.
func RunWithRetry(ctx context.Context, opName string, f func(ctx context.Context) error) error {
	_, err := RunWithRetryEx(ctx, opName, func(ctx context.Context) (any, error) {
		return nil, f(ctx)
	})
	return err
}

// RunWithRetryEx run function returning value with startup retry.
func RunWithRetryEx[T any](
	ctx context.Context, opName string, f func(ctx context.Context) (T, error),
) (T, error) {
	ctx = context.WithValue(ctx, startupRetryKey{}, "in retry")
	// Retry indefinitely until context is cancelled.
	var result T
	var err error
	for r := retry.Start(startupRetryOpts); r.Next(); {
		result, err = f(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil || !IsRetryableReplicaError(err) {
			break
		}
		if r.CurrentAttempt() > retriesBeforeLogging {
			log.Infof(ctx, "failed %s during node startup, retrying %s", opName, err)
		}
	}
	// This shouldn't happen as we retry indefinitely.
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
	if !inStartup() {
		log.Fatal(ctx, "startup tag context should only be called from server startup method")
	}
	runningStartup.Add(1)
	return func() {
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

const startupFuncName = "pkg/server.(*Server).PreStart"
const startupFuncNameLen = len(startupFuncName)

// NB: we use dumb startup check instead of peeking goid because the primary
// use case is detecting incorrect behaviour in tests. Those tests rely on
// TestCluster which could run node startup in parallel making it impossible to
// easily track global id.
func inStartup() bool {
	var ok = true
	for skip := 0; ok; skip++ {
		var pc uintptr
		pc, _, _, ok = runtime.Caller(skip)
		fd := runtime.FuncForPC(pc)
		if name := fd.Name(); len(name) > startupFuncNameLen && name[len(name)-startupFuncNameLen:] == startupFuncName {
			return true
		}
	}
	return false
}

// IsRetryableReplicaError returns true for replica availability errors.
func IsRetryableReplicaError(err error) bool {
	return errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil))
}
