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
	"runtime"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

type startupRetryKey struct{}

// RetryOpts defines retry options used when making kv requests on
// startup. These retries are necessary to ensure that circuit breaker that was
// tripped by the absence of node itself won't prevent it from starting.
var RetryOpts = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     3 * time.Second,
	Multiplier:     2,
}

// TODO(oleg): this is purely for testing purposes as of now.
var runningStartup atomic.Int32

// RetryContext adds startup tag to allow kv access from startup path.
func RetryContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, startupRetryKey{}, false)
}

func MaybeTagStartupContext(ctx context.Context) (context.Context, func()) {
	//if !util.RaceEnabled {
	//	return
	//}
	if !inStartup() {
		log.Fatal(ctx, "startup tag context should only be called from server startup method")
	}
	runningStartup.Add(1)
	return context.WithValue(ctx, startupRetryKey{}, true), func() {
		runningStartup.Add(-1)
	}
}

// ShouldRetryReplicas returns true if caller must perform replica error retries
// internally. Those errors must be retried on startup path to ensure node
// doesn't bail too early and prevent quorum being restored on ranges.
func ShouldRetryReplicas(ctx context.Context) bool {
	if runningStartup.Load() == 0 {
		// If startup is finished, we don't need any special retries.
		return false
	}
	r, ok := ctx.Value(startupRetryKey{}).(bool)
	if !ok && inStartup() {
		log.Fatal(ctx, "startup queries should be run within startup context")
	}
	return r
}

func AssertStartupTagged(ctx context.Context) {
	//if !util.RaceEnabled {
	//	return
	//}
	if runningStartup.Load() == 0 {
		return
	}
	if rv := ctx.Value(startupRetryKey{}); rv == nil && inStartup() {
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

func IsRetryableError(err error) bool {
	return errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil))
}
