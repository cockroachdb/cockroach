// Copyright 2018 The Cockroach Authors.
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
	"testing"
)

func TestSpanStatRegistry(t *testing.T) {
	sd := SpanStatDescriptorBase{Name: "test.stat"}

	if _, ok := LookupSpanStat("not.there"); ok {
		t.Fatal("unregistered stat found")
	}

	RegisterSpanStat(sd)
	if _, ok := LookupSpanStat(sd.Name); !ok {
		t.Fatal("registered stat not found")
	}
	// A lookup with SpanStatPrefix is valid.
	if _, ok := LookupSpanStat(SpanStatPrefix + sd.Name); !ok {
		t.Fatal("registered stat not found")
	}

	t.Run("DoubleRegistration", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("registered an already registered stat")
			}
		}()
		RegisterSpanStat(sd)
	})
}

func TestSpanStats(t *testing.T) {
	tr := NewTracer()
	os := tr.StartSpan("test", Recordable)

	span, ok := os.(*span)
	if !ok {
		t.Fatalf("unexpected span type %T", os)
	}
	// StartRecording, otherwise tags are not added to a span.
	StartRecording(span, SingleNodeRecording)

	t.Run("SetUnregistered", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("set an unregistered stat on a span")
			}
		}()
		SetSpanStat(span, "not.there", "value")
	})

	// Add tags that are not span stats.
	span.SetTag("not.a.stat", 1234)
	span.SetTag("because.no.prefix", "5678")
	if len(span.mu.tags) != 2 {
		t.Fatal("tags not added to span")
	}

	recordedSpans := GetRecording(span)
	if len(recordedSpans) != 1 {
		t.Fatalf("got %d recorded spans, expected 1", len(recordedSpans))
	}
	if spanStats := GetSpanStatsFromTags(recordedSpans[0].Tags); len(spanStats) != 0 {
		t.Fatal("expected no stats in span tags")
	}

	statDescriptors := []SpanStatDescriptorBase{
		{
			Name: "test.stat.one",
		},
		{
			Name: "test.stat.two",
		},
	}
	for _, sd := range statDescriptors {
		RegisterSpanStat(sd)
	}
	for _, sd := range statDescriptors {
		SetSpanStat(span, sd.Name, "value")
	}

	recordedSpans = GetRecording(span)
	if len(recordedSpans) != 1 {
		t.Fatalf("got %d recorded spans, expected 1", len(recordedSpans))
	}

	spanStats := GetSpanStatsFromTags(recordedSpans[0].Tags)
	if len(spanStats) != len(statDescriptors) {
		t.Fatalf("got %d stats in span, expected %d", len(spanStats), len(statDescriptors))
	}

	for _, sd := range statDescriptors {
		if _, ok := spanStats[sd.Name]; !ok {
			t.Fatalf("stat not found in span tags: %s, %v", sd.Name, spanStats)
		}
	}
}
