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

package log

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"golang.org/x/net/trace"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type events []string

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

// noLogV returns a verbosity level that will not result in VEvents and
// VErrEvents being logged.
func noLogV() int32 {
	return int32(logging.verbosity.get() + 1)
}

func TestTrace(t *testing.T) {
	ctx := context.Background()

	// The test below merely cares about observing events in traces.
	// Do not pollute the test's stderr with them.
	logging.stderrThreshold = Severity_FATAL

	// Events to context without a trace should be no-ops.
	Event(ctx, "should-not-show-up")

	tracer := tracing.NewTracer()
	tracer.SetForceRealSpans(true)
	sp := tracer.StartSpan("s")
	tracing.StartRecording(sp, tracing.SingleNodeRecording)
	ctxWithSpan := opentracing.ContextWithSpan(ctx, sp)
	Event(ctxWithSpan, "test1")
	VEvent(ctxWithSpan, noLogV(), "test2")
	VErrEvent(ctxWithSpan, noLogV(), "testerr")
	Info(ctxWithSpan, "log")

	// Events to parent context should still be no-ops.
	Event(ctx, "should-not-show-up")

	sp.Finish()

	if err := tracing.TestingCheckRecordedSpans(tracing.GetRecording(sp), `
		span s:
		  event: test1
		  event: test2
		  event: testerr
		  event: log
	`); err != nil {
		t.Fatal(err)
	}
}

func TestTraceWithTags(t *testing.T) {
	ctx := context.Background()
	ctx = WithLogTagInt(ctx, "tag", 1)

	tracer := tracing.NewTracer()
	tracer.SetForceRealSpans(true)
	sp := tracer.StartSpan("s")
	ctxWithSpan := opentracing.ContextWithSpan(ctx, sp)
	tracing.StartRecording(sp, tracing.SingleNodeRecording)

	Event(ctxWithSpan, "test1")
	VEvent(ctxWithSpan, noLogV(), "test2")
	VErrEvent(ctxWithSpan, noLogV(), "testerr")
	Info(ctxWithSpan, "log")

	sp.Finish()
	if err := tracing.TestingCheckRecordedSpans(tracing.GetRecording(sp), `
		span s:
		  event: [tag=1] test1
		  event: [tag=1] test2
		  event: [tag=1] testerr
		  event: [tag=1] log
	`); err != nil {
		t.Fatal(err)
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
	VEventf(ctxWithEventLog, noLogV(), "test%d", 2)
	VErrEvent(ctxWithEventLog, noLogV(), "testerr")
	Info(ctxWithEventLog, "log")
	Errorf(ctxWithEventLog, "errlog%d", 1)

	// Events to child contexts without the event log should be no-ops.
	Event(WithNoEventLog(ctxWithEventLog), "should-not-show-up")

	// Events to parent context should still be no-ops.
	Event(ctx, "should-not-show-up")

	FinishEventLog(ctxWithEventLog)

	// Events after Finish should be ignored.
	Errorf(ctxWithEventLog, "should-not-show-up")

	expected := events{"test1", "test2", "testerr(err)", "log", "errlog1(err)", "finish"}
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
	VEventf(ctxWithEventLog, noLogV(), "test2")
	VErrEvent(ctxWithEventLog, noLogV(), "testerr")

	tracer := tracing.NewTracer()
	tracer.SetForceRealSpans(true)
	sp := tracer.StartSpan("s")
	tracing.StartRecording(sp, tracing.SingleNodeRecording)
	ctxWithBoth := opentracing.ContextWithSpan(ctxWithEventLog, sp)
	// Events should only go to the trace.
	Event(ctxWithBoth, "test3")
	VEventf(ctxWithBoth, noLogV(), "test4")
	VErrEventf(ctxWithBoth, noLogV(), "%s", "test5err")

	// Events to parent context should still go to the event log.
	Event(ctxWithEventLog, "test6")

	sp.Finish()
	el.Finish()

	if err := tracing.TestingCheckRecordedSpans(tracing.GetRecording(sp), `
		span s:
		  event: test3
		  event: test4
		  event: test5err
	`); err != nil {
		t.Fatal(err)
	}

	elExpected := "[test1 test2 testerr(err) test6 finish]"
	if evStr := fmt.Sprint(el.ev); evStr != elExpected {
		t.Errorf("expected events '%s', got '%s'", elExpected, evStr)
	}
}
