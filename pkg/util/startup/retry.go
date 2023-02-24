// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package startup

import (
	"context"
	"reflect"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

type startupRetryKey struct{}

var startupRetryOpts = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     3 * time.Second,
	Multiplier:     2,
}

// runningStartup is a counter showing number of concurrently running server
// startups. if it is zero, then startups are finished and we should not try
// to run startup retry checks.
var runningStartup atomic.Int32

// RetryContext adds startup tag to allow kv access from startup path.
func RetryContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, startupRetryKey{}, "in retry")
}

// RunWithStartupRetry synchronously runs a function with retry of circuit
// breaker errors on startup.
func RunWithStartupRetry(
	ctx context.Context, opName string, f func(ctx context.Context) error,
) error {
	ctx = RetryContext(ctx)
	// We retry till success or till context is cancelled.
	var err error
	for r := retry.Start(startupRetryOpts); r.Next(); {
		err = f(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil || !IsRetryableReplicaError(err) {
			break
		}
		log.Infof(ctx, "failed %s during node startup, retrying (%s / %s)", opName, err, reflect.TypeOf(err))
	}
	return err
}

// RunWithStartupRetryEx run function returning value with startup retry.
func RunWithStartupRetryEx[T any](
	ctx context.Context, opName string, f func(ctx context.Context) (T, error),
) (T, error) {
	ctx = RetryContext(ctx)
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
		log.VEventf(ctx, 1, "failed %s during node startup, retrying %s", opName, err)
	}
	// This shouldn't happen as we retry indefinitely.
	return result, err
}

// RunWithoutRetry run function with context that allows kv execution query on
// startup. This is meant for parts of code that is only executed in tests
// e.g. code paths that avoid asynchronous execution or creating jobs and
// instead run eagerly during startup. This shouldn't be used to suppress
// startup assertions as makes server vulnerable to crash loops on startup.
func RunWithoutRetry(ctx context.Context, f func(ctx context.Context) error) error {
	ctx = RetryContext(ctx)
	return f(ctx)
}

func MaybeTagStartupContext(ctx context.Context) func() {
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

func AssertStartupRetry(ctx context.Context) {
	if !util.RaceEnabled {
		return
	}
	if runningStartup.Load() < 1 {
		return
	}

	rv := ctx.Value(startupRetryKey{})
	if rv == nil && inStartup() {
		log.Fatal(ctx, "startup query called outside of startup retry loop. See startup.RunWithStartupRetry()")
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

func IsRetryableReplicaError(err error) bool {
	return errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil))
}
