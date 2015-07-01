// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package tracer

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util"
	_ "github.com/cockroachdb/cockroach/util/log" // enable flags
)

type traceID int

func (id traceID) TraceID() string {
	return strconv.Itoa(int(id))
}

func (id traceID) TraceName() string {
	return id.TraceID()
}

func TestTracer(t *testing.T) {
	f := &util.Feed{}
	sub := f.Subscribe()
	const origin = ":8081"
	tracer := NewTracer(f, origin)

	now := &time.Time{}

	add := func(t time.Duration) {
		then := now.Add(t)
		now = &then
	}

	tracer.now = func() time.Time {
		return *now
	}

	// TODO(tschottdorf): Test some more.
	req1 := traceID(10)
	t1 := tracer.NewTrace(req1)

	e1a := t1.Epoch("A1")
	add(10 * time.Millisecond)
	e1b := t1.Epoch("A2")
	add(5 * time.Millisecond)
	t1.Event("E3")
	add(2 * time.Millisecond)
	t1.Event("E4")
	add(3 * time.Millisecond)
	e1b() // 10ms elapsed
	add(10 * time.Millisecond)
	e1a() // 30ms elapsed
	t1.Finalize()

	if !util.Panics(e1a) || !util.Panics(e1b) {
		t.Fatalf("expected a panic when ending an epoch twice")
	}

	f.Close()

	expTraces := []Trace{
		{ID: "10", Content: []TraceItem{
			{
				depth:    1,
				Name:     "A1",
				Duration: 30 * time.Millisecond,
			},
			{
				depth:     2,
				Timestamp: time.Time{}.Add(10 * time.Millisecond),
				Name:      "A2",
				Duration:  10 * time.Millisecond,
			},
			{
				depth:     3,
				Timestamp: time.Time{}.Add(15 * time.Millisecond),
				Name:      "E3",
			},
			{
				depth:     3,
				Timestamp: time.Time{}.Add(17 * time.Millisecond),
				Name:      "E4",
			},
		}},
	}
	for {
		event := <-sub.Events()
		if event == nil {
			break
		}
		trace := event.(*Trace)

		if len(expTraces) == 0 {
			t.Fatalf("unexpected extra trace: %s", trace)
		}
		expTrace := expTraces[0]
		expTraces = expTraces[1:]
		if !reflect.DeepEqual(expTrace.ID, trace.ID) {
			t.Errorf("expected ID %s, got %s", expTrace.ID, trace.ID)
		}
		tc := trace.Content
		for i, v := range tc {
			if !strings.Contains(v.Func, "TestTracer") || !strings.Contains(v.File, "tracer_test.go") {
				t.Errorf("invalid callsite in trace: %s %s", v.Func, v.File)
			}
			if v.Origin != origin {
				t.Fatalf("unexpected origin %s", v.Origin)
			}
			tc[i].Func, tc[i].File, tc[i].Origin = "", "", ""
		}
		if !reflect.DeepEqual(expTrace.Content, trace.Content) {
			t.Fatalf("unexpected content:\n%+v\nwanted:\n%+v", trace, expTrace)
		}
	}
	if len(expTraces) > 0 {
		t.Fatalf("missing traces:\n%+v", expTraces)
	}
}

func TestNilTracer(t *testing.T) {
	trace := (*Tracer)(nil).NewTrace(traceID(9))
	defer trace.Epoch("A")()
	trace.Event("B")
	trace.Event("C")
	trace.Event("D")
	trace.Epoch("E")()
}

func TestEpochBalance(t *testing.T) {
	trace := (*Tracer)(nil).NewTrace(traceID(9))
	trace.Epoch("never finalized")
	if !util.Panics(func() { trace.Finalize() }) {
		t.Fatalf("should panic when Finalize is called too early")
	}
}
