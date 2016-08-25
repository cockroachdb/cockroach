// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package log

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

type events []string

// testingTracer creates a Tracer that appends the events to the given slice.
func testingTracer(ev *events) opentracing.Tracer {
	opts := basictracer.DefaultOptions()
	opts.ShouldSample = func(_ uint64) bool { return true }
	opts.NewSpanEventListener = func() func(basictracer.SpanEvent) {
		return func(e basictracer.SpanEvent) {
			switch t := e.(type) {
			case basictracer.EventCreate:
				*ev = append(*ev, "start")
			case basictracer.EventFinish:
				*ev = append(*ev, "finish")
			case basictracer.EventLog:
				*ev = append(*ev, t.Event)
			}
		}
	}
	opts.DebugAssertUseAfterFinish = true
	opts.Recorder = &basictracer.InMemorySpanRecorder{}
	return basictracer.NewWithOptions(opts)
}

func TestTrace(t *testing.T) {
	ctx := context.Background()

	// Events to context without a trace should be no-ops.
	Event(ctx, "should-not-show-up")

	var ev events

	tracer := testingTracer(&ev)
	sp := tracer.StartSpan("")
	ctxWithSpan := opentracing.ContextWithSpan(ctx, sp)
	Event(ctxWithSpan, "test1")
	ErrEvent(ctxWithSpan, "testerr")
	VEvent(logging.verbosity.get()+1, ctxWithSpan, "test2")

	// Events to parent context should still be no-ops.
	Event(ctx, "should-not-show-up")

	sp.Finish()

	expected := "[start test1 testerr test2 finish]"
	if evStr := fmt.Sprint(ev); evStr != expected {
		t.Errorf("expected events '%s', got '%s'", expected, evStr)
	}
}

// testingEventLog is a simple implementation of trace.EventLog.
type testingEventLog struct {
	ev events
}

var _ trace.EventLog = &testingEventLog{}

func (el *testingEventLog) Printf(format string, a ...interface{}) {
	el.ev = append(el.ev, fmt.Sprintf(format, a...))
}

func (el *testingEventLog) Errorf(format string, a ...interface{}) {
	el.ev = append(el.ev, fmt.Sprintf(format+"(err)", a...))
}

func (el *testingEventLog) Finish() {
	el.ev = append(el.ev, "finish")
}

func TestEventLog(t *testing.T) {
	ctx := context.Background()

	// Events to context without a trace should be no-ops.
	Event(ctx, "should-not-show-up")

	el := &testingEventLog{}
	ctxWithEventLog := WithEventLog(ctx, el)

	Event(ctxWithEventLog, "test1")
	ErrEvent(ctxWithEventLog, "testerr")
	VEvent(logging.verbosity.get()+1, ctxWithEventLog, "test2")

	// Events to parent context should still be no-ops.
	Event(ctx, "should-not-show-up")

	el.Finish()

	expected := "[test1 testerr(err) test2 finish]"
	if evStr := fmt.Sprint(el.ev); evStr != expected {
		t.Errorf("expected events '%s', got '%s'", expected, evStr)
	}
}

func TestEventLogAndTrace(t *testing.T) {
	ctx := context.Background()

	// Events to context without a trace should be no-ops.
	Event(ctx, "should-not-show-up")

	el := &testingEventLog{}
	ctxWithEventLog := WithEventLog(ctx, el)

	Event(ctxWithEventLog, "test1")
	ErrEvent(ctxWithEventLog, "testerr")

	var traceEv events
	tracer := testingTracer(&traceEv)
	sp := tracer.StartSpan("")
	ctxWithBoth := opentracing.ContextWithSpan(ctxWithEventLog, sp)
	// Events should only go to the trace.
	Event(ctxWithBoth, "test3")
	ErrEventf(ctxWithBoth, "%s", "test3err")

	// Events to parent context should still go to the event log.
	Event(ctxWithEventLog, "test5")

	sp.Finish()
	el.Finish()

	trExpected := "[start test3 test3err finish]"
	if evStr := fmt.Sprint(traceEv); evStr != trExpected {
		t.Errorf("expected events '%s', got '%s'", trExpected, evStr)
	}

	elExpected := "[test1 testerr(err) test5 finish]"
	if evStr := fmt.Sprint(el.ev); evStr != elExpected {
		t.Errorf("expected events '%s', got '%s'", elExpected, evStr)
	}
}
