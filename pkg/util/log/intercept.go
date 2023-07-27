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
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// InterceptWith diverts log traffic to the given interceptor `fn`.
// `fn.Intercept()` is invoked for each log entry, regardless of the
// filtering configured on log sinks.
//
// The returned function should be called to cancel the interception.
//
// Multiple interceptors can be configured simultaneously.
// This enables e.g. concurrent uses of the "logspy" debug API.
// When multiple interceptors are configured, each of them
// are served each log entry.
func InterceptWith(ctx context.Context, fn Interceptor) func() {
	InfofDepth(ctx, 1, "starting log interception")
	logging.interceptor.add(fn)
	return func() {
		logging.interceptor.del(fn)
		InfofDepth(ctx, 1, "stopping log interception")
	}
}

// Interceptor is the type of an object that can be passed to
// InterceptWith().
type Interceptor interface {
	// Intercept is passed each log entries.
	// It is passed the entry payload in JSON format.
	// This can be converted to other formats by de-serializing the JSON
	// to logpb.Entry.
	//
	// Note that the argument byte buffer is only valid during the call
	// to Intercept(): it will be reused after the call to Intercept()
	// terminates. If the Intercept() implementation wants to share this
	// data across goroutines, it must take care of copying it first.
	Intercept(entry []byte)
}

func (l *loggingT) newInterceptorSinkInfo() *sinkInfo {
	si := &sinkInfo{
		sink:       &l.interceptor,
		editor:     getEditor(WithMarkedSensitiveData),
		formatter:  formatInterceptor{},
		redact:     false, // do not redact sensitive information
		redactable: true,  // keep redaction markers
	}
	// Ensure all events are collected across all channels.
	si.threshold.setAll(severity.INFO)
	return si
}

// formatInterceptor converts the raw logpb.Entry to JSON.
type formatInterceptor struct{}

func (formatInterceptor) formatterName() string { return "json-intercept" }
func (formatInterceptor) doc() string           { return "internal only" }
func (formatInterceptor) contentType() string   { return "application/json" }
func (formatInterceptor) formatEntry(entry logEntry) *buffer {
	pEntry := entry.convertToLegacy()
	buf := getBuffer()
	if j, err := json.Marshal(pEntry); err != nil {
		fmt.Fprintf(buf, "unable to format entry: %v", err)
	} else {
		buf.Write(j)
	}
	return buf
}

type interceptorSink struct {
	// activeCount is the number of functions under the mutex. We keep
	// it out to avoid locking the mutex in the active() method.
	activeCount uint32
	mu          struct {
		syncutil.RWMutex

		// fns is the list of interceptor functions.
		fns []Interceptor
	}
}

func (i *interceptorSink) add(fn Interceptor) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.mu.fns = append(i.mu.fns, fn)
	atomic.AddUint32(&i.activeCount, 1)
}

func (i *interceptorSink) del(toDel Interceptor) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for j, fn := range i.mu.fns {
		if fn == toDel {
			i.mu.fns = append(i.mu.fns[:j], i.mu.fns[j+1:]...)
			break
		}
	}
	atomic.AddUint32(&i.activeCount, ^uint32(0) /* -1 */)
}

func (i *interceptorSink) active() bool {
	return atomic.LoadUint32(&i.activeCount) > 0
}

func (i *interceptorSink) output(b []byte, _ sinkOutputOptions) error {
	i.mu.RLock()
	defer i.mu.RUnlock()
	for _, fn := range i.mu.fns {
		fn.Intercept(b)
	}
	return nil
}

func (i *interceptorSink) attachHints(stacks []byte) []byte { return stacks }
func (i *interceptorSink) exitCode() exit.Code              { return exit.UnspecifiedError() }
