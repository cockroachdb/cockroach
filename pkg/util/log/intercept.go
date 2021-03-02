// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TODO(yevgeniy): Remove/replace Intercept with interceptor sink implementation.
// Intercept diverts log traffic to the given function `f`. When `f` is not nil,
// the logging package begins operating at full verbosity (i.e. `V(n) == true`
// for all `n`) but nothing will be printed to the logs. Instead, `f` is invoked
// for each log entry.
//
// To end log interception, invoke `Intercept()` with `f == nil`. Note that
// interception does not terminate atomically, that is, the originally supplied
// callback may still be invoked after a call to `Intercept` with `f == nil`.
func Intercept(ctx context.Context, f InterceptorFn) {
	// TODO(tschottdorf): restore sanity so that all methods have a *loggingT
	// receiver.
	if f != nil {
		logfDepth(ctx, 1, severity.WARNING, channel.DEV, "log traffic is now intercepted; log files will be incomplete")
	}
	logging.interceptor.Store(f) // intentionally also when f == nil
	if f == nil {
		logfDepth(ctx, 1, severity.INFO, channel.DEV, "log interception is now stopped; normal logging resumes")
	}
}

// InterceptorFn is the type of function accepted by Intercept().
type InterceptorFn func(entry logpb.Entry)

// InterceptorID identifies interceptor.
type InterceptorID int64

// LogMessageInterceptor is an interface for intercepting log messages.
type LogMessageInterceptor interface {
	// Intercept receives intercepted message.
	// The implementation *must* meet the following requirements:
	//   * It must never block.
	//     Blocking may result in node crashes (e.g. due to disk stall detection)
	//   * Must not mutate message.
	//   * If buffering, the callback must make a copy of the message.
	Intercept(message []byte)
}

// interceptors is a synchronized structure containing the
// the list of active interceptors.
var interceptors = struct {
	isActive atomic.Value

	syncutil.Mutex
	nextID InterceptorID
	active map[InterceptorID]LogMessageInterceptor
}{
	active: make(map[InterceptorID]LogMessageInterceptor),
}

func haveActiveInterceptors() bool {
	a, ok := interceptors.isActive.Load().(bool)
	return a && ok
}

// AddInterceptor adds an interceptor to the list of interceptors.
func AddInterceptor(interceptor LogMessageInterceptor) InterceptorID {
	interceptors.Lock()
	defer interceptors.Unlock()
	id := interceptors.nextID
	interceptors.nextID++
	interceptors.active[id] = interceptor
	interceptors.isActive.Store(true)
	return id
}

// RemoveInterceptor removes previously added interceptor.
func RemoveInterceptor(id InterceptorID) {
	interceptors.Lock()
	defer interceptors.Unlock()

	delete(interceptors.active, id)
	interceptors.isActive.Store(len(interceptors.active) != 0)
}

// interceptSink is a log sink implementation that can send
// log messages to the interceptors.
type interceptSink struct{}

func (s *interceptSink) active() bool {
	return haveActiveInterceptors()
}

func (s *interceptSink) attachHints(stacks []byte) []byte {
	return stacks
}

func (s *interceptSink) output(extraSync bool, b []byte) error {
	// Ok to lock interceptors.  We're just locking out
	// Add/Remove interceptor calls.
	interceptors.Lock()
	defer interceptors.Unlock()
	for _, interceptor := range interceptors.active {
		interceptor.Intercept(b)
	}
	return nil
}

func (s *interceptSink) exitCode() exit.Code {
	return exit.LoggingNetCollectorUnavailable()
}

func (s *interceptSink) emergencyOutput(bytes []byte) {
}
