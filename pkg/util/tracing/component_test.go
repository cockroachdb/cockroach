// Copyright 2019 The Cockroach Authors.
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

package tracing

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroachdb/logtags"
)

func verifyActivity(t *testing.T, ca ComponentActivity, count, errors int64, samples int) {
	if ca.Count != count {
		t.Errorf("expected count %d; got %d", count, ca.Count)
	}
	if ca.Errors != errors {
		t.Errorf("expected errors %d; got %d", errors, ca.Errors)
	}
	if len(ca.Samples) != samples {
		t.Errorf("expected %d samples; got %d", samples, len(ca.Samples))
	}
}

func verifySample(
	t *testing.T,
	sample ComponentActivity_Sample,
	err string,
	attrs map[string]interface{},
	pending, stuck bool,
	spanEvents [][]string,
) {
	if sample.Error != err {
		t.Errorf("expected error %s; got %s", err, sample.Error)
	}
	for k, v := range attrs {
		if vStr := fmt.Sprint(v); sample.Attributes[k] != vStr {
			t.Errorf("expected Attributes[%s]=%q; got %q", k, vStr, sample.Attributes[k])
		}
	}
	if sample.Pending != pending {
		t.Errorf("expected sample pending %t; got %t", sample.Pending, pending)
	}
	if sample.Stuck != stuck {
		t.Errorf("expected sample stuck %t; got %t", sample.Stuck, stuck)
	}
	for i, events := range spanEvents {
		for j, e := range events {
			if field := sample.Spans[i].Logs[j].Fields[0]; field.Value != e {
				t.Errorf("expected span[%d].log[%d] = %s; got %s", i, j, e, field.Value)
			}
		}
	}
}

func startRecording() (chan time.Time, <-chan map[string]ComponentActivity) {
	stop := make(chan time.Time)
	resultsCh := make(chan map[string]ComponentActivity, 1)
	go func() {
		resultsCh <- Record(stop, 10)
	}()
	// Must wait for the recording to start.
	for {
		if isRecording() {
			break
		}
	}
	return stop, resultsCh
}

var (
	arg1 = "value1"
	arg2 = 2
	arg3 = []string{"foo", "bar", "baz"}
	out1 = "result1"
	out2 = []int64{20, 30}
)

func TestStartComponentTrace(t *testing.T) {
	tr := NewTracer()

	// Start a trace with no active recordings.
	rootCtx := context.Background()
	if _, span, _ := StartComponentTrace(rootCtx, tr, "test", false); IsRecording(span) {
		t.Fatal("component trace started but no recordings are active")
	}

	// Start a recording.
	stop, resultsCh := startRecording()

	_, span1, finish1 := StartComponentTrace(rootCtx, tr, "comp 1", false /* isStuck */)
	span1.SetTag("arg1", arg1)
	span1.SetTag("arg2", arg2)
	span1.LogEvent("event 1")
	span1.LogEvent("event 2")
	span1.SetTag("out1", out1)
	span1.SetTag("out2", out2)
	finish1(nil)

	_, span2, finish2 := StartComponentTrace(rootCtx, tr, "comp 2", false /* isStuck */)
	span2.SetTag("arg1", arg1)
	span2.LogEvent("event 1")
	span2.LogEvent("event 2")
	span2.LogEvent("event 3")
	span2.SetTag("out1", out1)
	finish2(errors.New("error2"))

	_, span3, finish3 := StartComponentTrace(rootCtx, tr, "comp 2", false /* isStuck */)
	span3.SetTag("arg3", arg3)
	span3.LogEvent("event 1")
	// Note: defer finish for the third trace.
	defer finish3(nil)

	// Stop the recorder and check results.
	close(stop)
	results := <-resultsCh

	// Check results include comp 1 and comp 2.
	ca1, ca2 := results["comp 1"], results["comp 2"]
	verifyActivity(t, ca1, 1, 0, 1)
	verifySample(t, ca1.Samples[0], "", map[string]interface{}{
		"arg1": arg1,
		"arg2": arg2,
		"out1": out1,
		"out2": out2,
	}, false, false, [][]string{{"event 1", "event 2"}})
	// Verify samples for comp 2.
	verifyActivity(t, ca2, 2, 1, 2)
	verifySample(t, ca2.Samples[0], "error2", map[string]interface{}{
		"arg1": arg1,
		"out1": out1,
	}, false, false, [][]string{{"event 1", "event 2", "event 3"}})
	verifySample(t, ca2.Samples[1], "", map[string]interface{}{
		"arg3": arg3,
	}, true, false, [][]string{{"event 1"}})
}

func TestStuckComponentTrace(t *testing.T) {
	tr := NewTracer()

	// Start a trace for a stuck component. This should create a trace
	// even without active recording(s).
	_, span, finish := StartComponentTrace(context.Background(), tr, "test", true /* isStuck */)
	if !IsRecording(span) {
		t.Error("component trace should have started since isStuck was specified as true")
	}
	defer finish(nil)
	span.SetTag("bar", 1)
	span.LogEvent("log message 1")

	// Start a recording and verify we collect the stuck trace.
	stop := make(chan struct{})
	close(stop)
	results := Record(stop, 10)

	if len(results) != 1 {
		t.Errorf("expected one result: %v", results)
	}
	ca, ok := results["test"]
	if !ok {
		t.Error("expected test component activity")
	}
	verifyActivity(t, ca, 0, 0, 1)
	verifySample(t, ca.Samples[0], "", map[string]interface{}{"bar": 1}, true, true, [][]string{{"log message 1"}})
}

func TestComponentTraceWithSubSpans(t *testing.T) {
	tr := NewTracer()

	// Start a recording.
	stop, resultsCh := startRecording()

	ctx, span, finish := StartComponentTrace(context.Background(), tr, "comp", false /* isStuck */)
	span.LogEvent("toplevel")

	cSpan1 := StartChildSpan("subspan", span, logtags.FromContext(ctx), false /* separateRecording */)
	cSpan1.LogEvent("sublevel 1")

	cSpan2 := StartChildSpan("subspan 2", cSpan1, logtags.FromContext(ctx), false /* separateRecording */)
	cSpan2.LogEvent("sublevel 2")
	cSpan2.Finish()

	cSpan1.Finish()
	finish(nil)

	// Stop the recorder and check results.
	close(stop)
	results := <-resultsCh

	ca := results["comp"]
	verifyActivity(t, ca, 1, 0, 1)
	verifySample(t, ca.Samples[0], "", map[string]interface{}{}, false, false,
		[][]string{{"toplevel"}, {"sublevel 1"}, {"sublevel 2"}})
}

func TestComponentTraceWithSubComponents(t *testing.T) {
	tr := NewTracer()

	// Start a recording.
	stop, resultsCh := startRecording()

	ctx1, span1, finish1 := StartComponentTrace(context.Background(), tr, "comp 1", false /* isStuck */)
	span1.LogEvent("comp 1 message")

	ctx2, span2, finish2 := StartComponentTrace(ctx1, tr, "comp 2", false /* isStuck */)
	span2.LogEvent("comp 2 message")

	_, span3, finish3 := StartComponentTrace(ctx2, tr, "comp 3", false /* isStuck */)
	span3.LogEvent("comp 3 message")

	defer finish1(nil) // wait on finish 1
	finish2(errors.New("error on comp 2"))
	finish3(nil)

	// Stop the recorder and check results.
	close(stop)
	results := <-resultsCh

	ca1 := results["comp 1"]
	verifyActivity(t, ca1, 1, 0, 1)
	verifySample(t, ca1.Samples[0], "", map[string]interface{}{}, true, false, [][]string{{"comp 1 message"}})

	ca2 := results["comp 2"]
	verifyActivity(t, ca2, 1, 1, 1)
	verifySample(t, ca2.Samples[0], "error on comp 2", map[string]interface{}{}, false, false, [][]string{{"comp 2 message"}})

	ca3 := results["comp 3"]
	verifyActivity(t, ca3, 1, 0, 1)
	verifySample(t, ca3.Samples[0], "", map[string]interface{}{}, false, false, [][]string{{"comp 3 message"}})
}

func TestComponentTraceSampling(t *testing.T) {
	tr := NewTracer()

	// Start a recording.
	stop, resultsCh := startRecording()

	const numSamples = 100
	for i := 0; i < numSamples; i++ {
		_, span, finish := StartComponentTrace(context.Background(), tr, "comp", false /* isStuck */)
		span.LogEvent(fmt.Sprintf("message %d", i))
		finish(nil)
	}

	// Stop the recorder and check results.
	close(stop)
	results := <-resultsCh

	ca := results["comp"]
	lenSamples := len(ca.Samples)
	if lenSamples == numSamples {
		t.Errorf("expected less than %d samples", numSamples)
	}
	verifyActivity(t, ca, numSamples, 0, lenSamples)
}
