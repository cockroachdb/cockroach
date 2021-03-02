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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
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
type InterceptorID uintptr

// InterceptMessageFn is a message interceptor callback.
// The callback *must* meet the following requirements:
//   * It must never block.
//     Blocking may result in node crashes (e.g. due to disk stall detection)
//   * Must not mutate message
//   * If buffering, the callback must make a copy of the message.
type InterceptMessageFn func(message []byte)

// interceptorsPtr stores a pointer to the list of currently active interceptors.
type interceptorsList []*InterceptMessageFn

var interceptorsPtr unsafe.Pointer

// loadInterceptors loads current interceptor list, and returns the list and its address.
func loadInterceptors() (unsafe.Pointer, *interceptorsList) {
	addr := atomic.LoadPointer(&interceptorsPtr)
	return addr, (*interceptorsList)(addr)
}

// interceptorID returns the ID for the specified interceptor.
func interceptorID(interceptor *InterceptMessageFn) InterceptorID {
	return InterceptorID(unsafe.Pointer(interceptor))
}

// add adds specified interceptor to the current set of interceptors and
// returns a new interceptor list along with the interceptor ID.
func (l *interceptorsList) add(interceptor InterceptMessageFn) (interceptorsList, InterceptorID) {
	newList := interceptorsList{&interceptor}
	if l != nil {
		newList = append(newList, *l...)
	}
	return newList, interceptorID(&interceptor)
}

// remove deletes specified interceptor from the interceptor list and returns
// new interceptor list.
func (l *interceptorsList) remove(id InterceptorID) interceptorsList {
	var newList interceptorsList
	yank := -1
	if l != nil {
		for k, v := range *l {
			if interceptorID(v) == id {
				yank = k
				break
			}
		}
	}
	if yank == -1 {
		panic("cannot remove interceptor")
	}
	if len(*l) > 1 {
		newList = make(interceptorsList, len(*l)-1)
		copy(newList, (*l)[:yank])
		copy(newList[yank:], (*l)[yank+1:])
	}
	return newList
}

// AddInterceptor adds an interceptor to the list of interceptors.
func AddInterceptor(interceptor InterceptMessageFn) InterceptorID {
	for {
		addr, oldInterceptors := loadInterceptors()
		newInterceptors, id := oldInterceptors.add(interceptor)
		if atomic.CompareAndSwapPointer(&interceptorsPtr, addr, unsafe.Pointer(&newInterceptors)) {
			return id
		}
	}
}

// RemoveInterceptor removes previously added interceptor.
func RemoveInterceptor(id InterceptorID) {
	for {
		addr, oldInterceptors := loadInterceptors()
		newInterceptors := oldInterceptors.remove(id)
		var newInterceptorsPtr *interceptorsList
		if len(newInterceptors) > 0 {
			newInterceptorsPtr = &newInterceptors
		}
		if atomic.CompareAndSwapPointer(&interceptorsPtr, addr, unsafe.Pointer(newInterceptorsPtr)) {
			return
		}
	}
}

// interceptSink is a log sink implementation that can send
// log messages to the interceptors.
type interceptSink struct{}

func (s *interceptSink) active() bool {
	_, interceptors := loadInterceptors()
	return interceptors != nil
}

func (s *interceptSink) attachHints(stacks []byte) []byte {
	return stacks
}

func (s *interceptSink) output(extraSync bool, b []byte) error {
	_, interceptors := loadInterceptors()
	if interceptors != nil {
		for _, interceptor := range *interceptors {
			(*interceptor)(b)
		}
	}
	return nil
}

func (s *interceptSink) exitCode() exit.Code {
	return exit.LoggingNetCollectorUnavailable()
}

func (s *interceptSink) emergencyOutput(bytes []byte) {
}
