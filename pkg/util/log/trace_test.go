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
	"regexp"
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
		// The op variable is used in the returned function and is associated with
		// a span; it lives for as long as the span is open.
		var op string
		return func(e basictracer.SpanEvent) {
			switch t := e.(type) {
			case basictracer.EventCreate:
				op = t.OperationName
				*ev = append(*ev, fmt.Sprintf("%s:start", op))
			case basictracer.EventFinish:
				*ev = append(*ev, fmt.Sprintf("%s:finish", op))
			case basictracer.EventLogFields:
				*ev = append(*ev, fmt.Sprintf("%s:%s", op, t.Fields[0].Value()))
			case basictracer.EventLog:
				panic("EventLog is deprecated")
			}
		}
	}
	opts.DebugAssertUseAfterFinish = true
	// We don't care about the recorder but we need to set it to something.
	opts.Recorder = &basictracer.InMemorySpanRecorder{}
	return basictracer.NewWithOptions(opts)
}

func compareTraces(expected, actual events) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i, ev := range expected {
		if ev == actual[i] {
			continue
		}
		// Try to strip file:line from a span event, e.g. from:
		//   span:path/file:line msg
		// to:
		//   span:msg
		groups := regexp.MustCompile(`^(.*):.*:[0-9]* (.*)$`).FindStringSubmatch(actual[i])
		if len(groups) == 3 && fmt.Sprintf("%s:%s", groups[1], groups[2]) == ev {
			continue
		}
		// Try to strip file:line from a non-span event, e.g. from:
		//   path/file:line msg
		// to:
		//   msg
		groups = regexp.MustCompile(`^.*:[0-9]* (.*)$`).FindStringSubmatch(actual[i])
		if len(groups) == 2 && groups[1] == ev {
			continue
		}
		return false
	}
	return true
}

func TestTrace(t *testing.T) {
	ctx := context.Background()

	// The test below merely cares about observing events in traces.
	// Do not pollute the test's stderr with them.
	logging.stderrThreshold = Severity_FATAL

	// Events to context without a trace should be no-ops.
	Event(ctx, "should-not-show-up")

	var ev events

	tracer := testingTracer(&ev)
	sp := tracer.StartSpan("s")
	ctxWithSpan := opentracing.ContextWithSpan(ctx, sp)
	Event(ctxWithSpan, "test1")
	ErrEvent(ctxWithSpan, "testerr")
	VEvent(ctxWithSpan, logging.verbosity.get()+1, "test2")
	Info(ctxWithSpan, "log")

	// Events to parent context should still be no-ops.
	Event(ctx, "should-not-show-up")

	sp.Finish()

	expected := events{"s:start", "s:test1", "s:testerr", "s:test2", "s:log", "s:finish"}
	if !compareTraces(expected, ev) {
		t.Errorf("expected events '%s', got '%v'", expected, ev)
	}
}

func TestTraceWithTags(t *testing.T) {
	ctx := context.Background()
	ctx = WithLogTagInt(ctx, "tag", 1)

	var ev events

	tracer := testingTracer(&ev)
	sp := tracer.StartSpan("s")
	ctxWithSpan := opentracing.ContextWithSpan(ctx, sp)
	Event(ctxWithSpan, "test1")
	ErrEvent(ctxWithSpan, "testerr")
	VEvent(ctxWithSpan, logging.verbosity.get()+1, "test2")
	Info(ctxWithSpan, "log")

	sp.Finish()

	expected := events{
		"s:start", "s:[tag=1] test1", "s:[tag=1] testerr", "s:[tag=1] test2", "s:[tag=1] log",
		"s:finish",
	}
	if !compareTraces(expected, ev) {
		t.Errorf("expected events '%s', got '%v'", expected, ev)
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
	ctxWithEventLog := withEventLogInternal(ctx, el)

	Eventf(ctxWithEventLog, "test%d", 1)
	ErrEvent(ctxWithEventLog, "testerr")
	VEventf(ctxWithEventLog, logging.verbosity.get()+1, "test%d", 2)
	Info(ctxWithEventLog, "log")
	Errorf(ctxWithEventLog, "errlog%d", 1)

	// Events to child contexts without the event log should be no-ops.
	Event(WithNoEventLog(ctxWithEventLog), "should-not-show-up")

	// Events to parent context should still be no-ops.
	Event(ctx, "should-not-show-up")

	FinishEventLog(ctxWithEventLog)

	// Events after Finish should be ignored.
	Errorf(ctxWithEventLog, "should-not-show-up")

	expected := events{"test1", "testerr(err)", "test2", "log", "errlog1(err)", "finish"}
	if !compareTraces(expected, el.ev) {
		t.Errorf("expected events '%s', got '%v'", expected, el.ev)
	}
}

func TestEventLogAndTrace(t *testing.T) {
	ctx := context.Background()

	// Events to context without a trace should be no-ops.
	Event(ctx, "should-not-show-up")

	el := &testingEventLog{}
	ctxWithEventLog := withEventLogInternal(ctx, el)

	Event(ctxWithEventLog, "test1")
	ErrEvent(ctxWithEventLog, "testerr")

	var traceEv events
	tracer := testingTracer(&traceEv)
	sp := tracer.StartSpan("s")
	ctxWithBoth := opentracing.ContextWithSpan(ctxWithEventLog, sp)
	// Events should only go to the trace.
	Event(ctxWithBoth, "test3")
	ErrEventf(ctxWithBoth, "%s", "test3err")

	// Events to parent context should still go to the event log.
	Event(ctxWithEventLog, "test5")

	sp.Finish()
	el.Finish()

	trExpected := "[s:start s:test3 s:test3err s:finish]"
	if evStr := fmt.Sprint(traceEv); evStr != trExpected {
		t.Errorf("expected events '%s', got '%s'", trExpected, evStr)
	}

	elExpected := "[test1 testerr(err) test5 finish]"
	if evStr := fmt.Sprint(el.ev); evStr != elExpected {
		t.Errorf("expected events '%s', got '%s'", elExpected, evStr)
	}
}
