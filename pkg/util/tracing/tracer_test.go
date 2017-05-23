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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
)

func TestEncodeDecodeRawSpan(t *testing.T) {
	s := basictracer.RawSpan{
		Context: basictracer.SpanContext{
			TraceID: 1,
			SpanID:  2,
			Sampled: true,
			Baggage: make(map[string]string),
		},
		ParentSpanID: 13,
		Operation:    "testop",
		Start:        time.Now(),
		Duration:     15 * time.Millisecond,
		Tags:         make(map[string]interface{}),
		Logs: []opentracing.LogRecord{
			{
				Timestamp: time.Now().Add(2 * time.Millisecond),
			},
			{
				Timestamp: time.Now().Add(5 * time.Millisecond),
				Fields: []otlog.Field{
					otlog.Int("f1", 3),
					otlog.String("f2", "f2Val"),
				},
			},
		},
	}
	s.Context.Baggage["bag"] = "bagVal"
	s.Tags["tag"] = 5

	enc, err := EncodeRawSpan(&s, nil)
	if err != nil {
		t.Fatal(err)
	}
	var d basictracer.RawSpan
	if err = DecodeRawSpan(enc, &d); err != nil {
		t.Fatal(err)
	}
	// We cannot use DeepEqual because we encode all log fields as strings. So
	// e.g. the "f1" field above is now a string, not an int. The string
	// representations must match though.
	sStr := fmt.Sprint(s)
	dStr := fmt.Sprint(d)
	if sStr != dStr {
		t.Errorf("initial span: '%s', after encode/decode: '%s'", sStr, dStr)
	}
}

func checkRawSpans(t *testing.T, rawSpans []basictracer.RawSpan, expected string) {
	expected = strings.TrimSpace(expected)
	var rows []string
	row := func(format string, args ...interface{}) {
		rows = append(rows, fmt.Sprintf(format, args...))
	}

	for _, rs := range rawSpans {
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
				msg = msg + fmt.Sprintf("  %s: %v", f.Key(), f.Value())
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

	checkRawSpans(t, GetRecording(s1), `
	  span a:
      x: 2
	  span b:
      x: 3
	`)

	checkRawSpans(t, GetRecording(s2), `
	  span a:
      x: 2
	  span b:
      x: 3
	`)

	s3 := tr.StartSpan("c", opentracing.FollowsFrom(s2.Context()))
	s3.LogKV("x", 4)
	s3.SetTag("tag", "val")

	s2.Finish()

	checkRawSpans(t, GetRecording(s1), `
	  span a:
      x: 2
	  span b:
      x: 3
	  span c:
		  tags: tag=val
      x: 4
	`)
	s3.Finish()
	checkRawSpans(t, GetRecording(s1), `
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
	checkRawSpans(t, GetRecording(s1), ``)
	// The child span is still recording.
	s3.LogKV("x", 5)
	checkRawSpans(t, GetRecording(s3), `
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
	carrier := &SpanContextCarrier{}
	if err := tr.Inject(noop1.Context(), basictracer.Delegator, carrier); err != nil {
		t.Fatal(err)
	}
	if carrier.TraceID != 0 {
		t.Errorf("noop span has carrier: %+v", carrier)
	}
	_, noop2, err := JoinRemoteTrace(context.Background(), tr2, carrier, "remote op")
	if err != nil {
		t.Fatal(err)
	}
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

	carrier = &SpanContextCarrier{}
	if err := tr.Inject(s1.Context(), basictracer.Delegator, carrier); err != nil {
		t.Fatal(err)
	}

	_, s2, err := JoinRemoteTrace(context.Background(), tr2, carrier, "remote op")
	if err != nil {
		t.Fatal(err)
	}
	// Compare TraceIDs
	trace1 := s1.Context().(*spanContext).TraceID
	trace2 := s2.Context().(*spanContext).TraceID
	if trace1 != trace2 {
		t.Errorf("TraceID doesn't match: parent %d child %d", trace1, trace2)
	}
	s2.LogKV("x", 1)

	// Verify that recording was started automatically.
	rec := GetRecording(s2)
	checkRawSpans(t, rec, `
	  span remote op:
	    x: 1
	`)

	checkRawSpans(t, GetRecording(s1), `
	  span a:
		  tags: sb=1
	`)

	// Encode spans as if we are sending them over the wire.
	var encSpans [][]byte
	for i := range rec {
		enc, err := EncodeRawSpan(&rec[i], nil)
		if err != nil {
			t.Fatal(err)
		}
		encSpans = append(encSpans, enc)
	}

	ctx := opentracing.ContextWithSpan(context.Background(), s1)
	if err := IngestRemoteSpans(ctx, encSpans); err != nil {
		t.Fatal(err)
	}

	checkRawSpans(t, GetRecording(s1), `
	  span a:
		  tags: sb=1
		span remote op:
			x: 1
	`)
}
