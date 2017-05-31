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

package tracing

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	lightstep "github.com/lightstep/lightstep-tracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

func checkRecordedSpans(t *testing.T, recSpans []RecordedSpan, expected string) {
	expected = strings.TrimSpace(expected)
	var rows []string
	row := func(format string, args ...interface{}) {
		rows = append(rows, fmt.Sprintf(format, args...))
	}

	for _, rs := range recSpans {
		row("span %s:", rs.Operation)
		if len(rs.Tags) > 0 {
			var tags []string
			for k, v := range rs.Tags {
				tags = append(tags, fmt.Sprintf("%s=%v", k, v))
			}
			sort.Strings(tags)
			row("  tags: %s", strings.Join(tags, " "))
		}
		for _, l := range rs.Logs {
			msg := ""
			for _, f := range l.Fields {
				msg = msg + fmt.Sprintf("  %s: %v", f.Key, f.Value)
			}
			row("%s", msg)
		}
	}
	var expRows []string
	if expected != "" {
		expRows = strings.Split(expected, "\n")
	}
	match := false
	if len(expRows) == len(rows) {
		match = true
		for i := range expRows {
			e := strings.Trim(expRows[i], " \t")
			r := strings.Trim(rows[i], " \t")
			if e != r {
				match = false
				break
			}
		}
	}
	if !match {
		file, line, _ := caller.Lookup(1)
		t.Errorf("%s:%d expected:\n%s\ngot:\n%s", file, line, expected, strings.Join(rows, "\n"))
	}
}

func TestTracerRecording(t *testing.T) {
	tr := newTracer(false /* netTrace */, nil /* lightstep */)

	noop1 := tr.StartSpan("noop")
	if !IsNoopSpan(noop1) {
		t.Error("expected noop span")
	}
	noop1.LogKV("hello", "void")

	noop2 := tr.StartSpan("noop2", opentracing.ChildOf(noop1.Context()))
	if !IsNoopSpan(noop2) {
		t.Error("expected noop child span")
	}
	noop2.Finish()
	noop1.Finish()

	s1 := tr.StartSpan("a", Recordable)
	if IsNoopSpan(s1) {
		t.Error("Recordable span should not be noop")
	}

	// Unless recording is actually started, child spans are still noop.
	noop3 := tr.StartSpan("noop3", opentracing.ChildOf(s1.Context()))
	if !IsNoopSpan(noop3) {
		t.Error("expected noop child span")
	}
	noop3.Finish()

	s1.LogKV("x", 1)
	StartRecording(s1)
	s1.LogKV("x", 2)
	s2 := tr.StartSpan("b", opentracing.ChildOf(s1.Context()))
	if IsNoopSpan(s2) {
		t.Error("recording span should not be noop")
	}
	s2.LogKV("x", 3)

	checkRecordedSpans(t, GetRecording(s1), `
	  span a:
      x: 2
	  span b:
      x: 3
	`)

	checkRecordedSpans(t, GetRecording(s2), `
	  span a:
      x: 2
	  span b:
      x: 3
	`)

	s3 := tr.StartSpan("c", opentracing.FollowsFrom(s2.Context()))
	s3.LogKV("x", 4)
	s3.SetTag("tag", "val")

	s2.Finish()

	checkRecordedSpans(t, GetRecording(s1), `
	  span a:
      x: 2
	  span b:
      x: 3
	  span c:
		  tags: tag=val
      x: 4
	`)
	s3.Finish()
	checkRecordedSpans(t, GetRecording(s1), `
	  span a:
      x: 2
	  span b:
      x: 3
	  span c:
		  tags: tag=val
      x: 4
	`)
	StopRecording(s1)
	s1.LogKV("x", 100)
	checkRecordedSpans(t, GetRecording(s1), ``)
	// The child span is still recording.
	s3.LogKV("x", 5)
	checkRecordedSpans(t, GetRecording(s3), `
	  span a:
      x: 2
	  span b:
      x: 3
	  span c:
		  tags: tag=val
      x: 4
      x: 5
	`)
	s1.Finish()
}

func TestTracerInjectExtract(t *testing.T) {
	tr := newTracer(false /* netTrace */, nil /* lightstep */)
	tr2 := newTracer(false /* netTrace */, nil /* lightstep */)

	// Verify that noop spans become noop spans on the remote side.

	noop1 := tr.StartSpan("noop")
	if !IsNoopSpan(noop1) {
		t.Fatalf("expected noop span: %+v", noop1)
	}
	carrier := make(opentracing.HTTPHeadersCarrier)
	if err := tr.Inject(noop1.Context(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}
	if len(carrier) != 0 {
		t.Errorf("noop span has carrier: %+v", carrier)
	}

	wireContext, err := tr2.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}
	if _, noopCtx := wireContext.(noopSpanContext); !noopCtx {
		t.Errorf("expected noop context: %v", wireContext)
	}
	noop2 := tr2.StartSpan("remote op", opentracing.FollowsFrom(wireContext))
	if !IsNoopSpan(noop2) {
		t.Fatalf("expected noop span: %+v", noop2)
	}
	noop1.Finish()
	noop2.Finish()

	// Verify that snowball tracing is propagated and triggers recording on the
	// remote side.

	s1 := tr.StartSpan("a", Recordable)
	StartRecording(s1)
	s1.SetBaggageItem(Snowball, "1")

	carrier = make(opentracing.HTTPHeadersCarrier)
	if err := tr.Inject(s1.Context(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}

	wireContext, err = tr2.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}
	s2 := tr2.StartSpan("remote op", opentracing.FollowsFrom(wireContext))

	// Compare TraceIDs
	trace1 := s1.Context().(*spanContext).TraceID
	trace2 := s2.Context().(*spanContext).TraceID
	if trace1 != trace2 {
		t.Errorf("TraceID doesn't match: parent %d child %d", trace1, trace2)
	}
	s2.LogKV("x", 1)

	// Verify that recording was started automatically.
	rec := GetRecording(s2)
	checkRecordedSpans(t, rec, `
	  span remote op:
	    x: 1
	`)

	checkRecordedSpans(t, GetRecording(s1), `
	  span a:
		  tags: sb=1
	`)

	if err := ImportRemoteSpans(s1, rec); err != nil {
		t.Fatal(err)
	}

	checkRecordedSpans(t, GetRecording(s1), `
	  span a:
		  tags: sb=1
		span remote op:
			x: 1
	`)
}

func TestLightstepContext(t *testing.T) {
	lsTr := lightstep.NewTracer(lightstep.Options{
		AccessToken: "invalid",
		Collector: lightstep.Endpoint{
			Host:      "127.0.0.1",
			Port:      65535,
			Plaintext: true,
		},
		MaxLogsPerSpan: maxLogsPerSpan,
		UseGRPC:        true,
	})
	tr := newTracer(false /* netTrace */, lsTr)
	s := tr.StartSpan("test")

	const testBaggageKey = "test-baggage"
	const testBaggageVal = "test-val"
	s.SetBaggageItem(testBaggageKey, testBaggageVal)

	carrier := make(opentracing.HTTPHeadersCarrier)
	if err := tr.Inject(s.Context(), opentracing.HTTPHeaders, carrier); err != nil {
		t.Fatal(err)
	}
	traceID, spanID := tr.getLightstepSpanIDs(s.(*span).lightstep.Context())
	if traceID == 0 || spanID == 0 {
		t.Errorf("invalid trace/span IDs: %d %d", traceID, spanID)
	}

	// Extract also extracts the context in lightstep; this will fail if the
	// contexts are not compatible.
	wireContext, err := tr.Extract(opentracing.HTTPHeaders, carrier)
	if err != nil {
		t.Fatal(err)
	}

	s2 := tr.StartSpan("child", opentracing.FollowsFrom(wireContext))
	s2Ctx := s2.(*span).lightstep.Context()

	traceID2, spanID2 := tr.getLightstepSpanIDs(s2Ctx)

	if traceID2 != traceID || spanID2 == 0 {
		t.Errorf("invalid child trace/span IDs: %d %d", traceID2, spanID2)
	}

	// Verify that the baggage is correct in both the tracer context and in the
	// lightstep context.
	for i, spanCtx := range []opentracing.SpanContext{s2.Context(), s2Ctx} {
		baggage := make(map[string]string)
		spanCtx.ForeachBaggageItem(func(k, v string) bool {
			baggage[k] = v
			return true
		})
		if len(baggage) != 1 || baggage[testBaggageKey] != testBaggageVal {
			t.Errorf("%d: expected baggage %s=%s, got %v", i, testBaggageKey, testBaggageVal, baggage)
		}
	}
}
