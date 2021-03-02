// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"golang.org/x/net/trace"
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
	VEventf(ctxWithEventLog, NoLogV(), "test%d", 2)
	VErrEvent(ctxWithEventLog, NoLogV(), "testerr")
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
	VEventf(ctxWithEventLog, NoLogV(), "test2")
	VErrEvent(ctxWithEventLog, NoLogV(), "testerr")

	tracer := tracing.NewTracer()
	sp := tracer.StartSpan("s", tracing.WithForceRealSpan())
	sp.SetVerbose(true)
	ctxWithBoth := tracing.ContextWithSpan(ctxWithEventLog, sp)
	// Events should only go to the trace.
	Event(ctxWithBoth, "test3")
	VEventf(ctxWithBoth, NoLogV(), "test4")
	VErrEventf(ctxWithBoth, NoLogV(), "%s", "test5err")

	// Events to parent context should still go to the event log.
	Event(ctxWithEventLog, "test6")

	sp.Finish()
	el.Finish()

	if err := tracing.TestingCheckRecordedSpans(sp.GetRecording(), `
		span: s
			tags: _verbose=1
			event: test3
			event: test4
			event: test5err
	`); err != nil {
		t.Fatal(err)
	}

	elExpected := regexp.MustCompile(`^\[` +
		`util/log/trace_test\.go:\d+ test1 ` +
		`util/log/trace_test\.go:\d+ test2 ` +
		`util/log/trace_test\.go:\d+ testerr\(err\) ` +
		`util/log/trace_test\.go:\d+ test6 ` +
		`finish\]$`)
	if evStr := fmt.Sprint(el.ev); !elExpected.MatchString(evStr) {
		t.Errorf("expected events '%s', got '%s'", elExpected, evStr)
	}
}
