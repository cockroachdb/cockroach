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
// Author: Peter Mattis (peter@cockroachlabs.com)

package tracing

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
)

type traceLogData struct {
	opentracing.LogData
	depth int
}

type traceLogs []traceLogData

func (l traceLogs) Len() int {
	return len(l)
}

func (l traceLogs) Less(i, j int) bool {
	return l[i].Timestamp.Before(l[j].Timestamp)
}

func (l traceLogs) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// FormatRawSpans formats the given spans for human consumption, showing the
// relationship using nesting and times as both relative to the previous event
// and cumulative.
func FormatRawSpans(spans []basictracer.RawSpan) string {
	m := make(map[uint64]*basictracer.RawSpan)
	for i, sp := range spans {
		m[sp.SpanID] = &spans[i]
	}

	var depth func(uint64) int
	depth = func(parentID uint64) int {
		p := m[parentID]
		if p == nil {
			return 0
		}
		return depth(p.ParentSpanID) + 1
	}

	var logs traceLogs
	var start time.Time
	for _, sp := range spans {
		if sp.ParentSpanID == 0 {
			start = sp.Start
		}
		d := depth(sp.ParentSpanID)
		for _, e := range sp.Logs {
			logs = append(logs, traceLogData{LogData: e, depth: d})
		}
	}
	sort.Sort(logs)

	var buf bytes.Buffer
	var last time.Time
	if len(logs) > 0 {
		last = logs[0].Timestamp
	}
	for _, entry := range logs {
		fmt.Fprintf(&buf, "% 10.3fms % 10.3fms%s%s\n",
			1000*entry.Timestamp.Sub(start).Seconds(),
			1000*entry.Timestamp.Sub(last).Seconds(),
			strings.Repeat("    ", entry.depth+1),
			entry.Event)
		last = entry.Timestamp
	}
	return buf.String()
}
