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

package tracing

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
)

type traceLogData struct {
	opentracing.LogRecord
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

// FormatRecordedSpans formats the given spans for human consumption, showing the
// relationship using nesting and times as both relative to the previous event
// and cumulative.
//
// TODO(andrei): this should be unified with
// SessionTracing.GenerateSessionTraceVTable.
func FormatRecordedSpans(spans []RecordedSpan) string {
	m := make(map[uint64]*RecordedSpan)
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
			start = sp.StartTime
		}
		d := depth(sp.ParentSpanID)
		// Issue a log with the operation name. Include any tags.
		lr := opentracing.LogRecord{
			Timestamp: sp.StartTime,
			Fields:    []otlog.Field{otlog.String("operation", sp.Operation)},
		}
		if len(sp.Tags) > 0 {
			tags := make([]string, 0, len(sp.Tags))
			for k := range sp.Tags {
				tags = append(tags, k)
			}
			sort.Strings(tags)
			for _, k := range tags {
				lr.Fields = append(lr.Fields, otlog.String(k, sp.Tags[k]))
			}
		}
		logs = append(logs, traceLogData{LogRecord: lr, depth: d})
		for _, l := range sp.Logs {
			lr := opentracing.LogRecord{
				Timestamp: l.Time,
				Fields:    make([]otlog.Field, len(l.Fields)),
			}
			for i, f := range l.Fields {
				lr.Fields[i] = otlog.String(f.Key, f.Value)
			}

			logs = append(logs, traceLogData{LogRecord: lr, depth: d})
		}
	}
	sort.Sort(logs)

	var buf bytes.Buffer
	last := start
	for _, entry := range logs {
		fmt.Fprintf(&buf, "% 10.3fms % 10.3fms%s",
			1000*entry.Timestamp.Sub(start).Seconds(),
			1000*entry.Timestamp.Sub(last).Seconds(),
			strings.Repeat("    ", entry.depth+1))
		for i, f := range entry.Fields {
			if i != 0 {
				buf.WriteByte(' ')
			}
			fmt.Fprintf(&buf, "%s:%v", f.Key(), f.Value())
		}
		buf.WriteByte('\n')
		last = entry.Timestamp
	}
	return buf.String()
}
