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
// Author: Andrei Matei (andreimatei1@gmail.com)

package sql

import (
	"fmt"
	"sort"
	"time"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

// sessionInfo is a virtual database exposing per-session information.
var sessionInfo = virtualSchema{
	name: "session_info",
	tables: []virtualSchemaTable{
		sessionInfoLatestTraceTable,
	},
}

// sessionInfoLatestTraceTable exposes the latest trace collected on this
// session (via SET TRACE={ON/OFF})
var sessionInfoLatestTraceTable = virtualSchemaTable{
	schema: `
CREATE TABLE session_info.latest_trace (
  CUMMULATIVE_DURATION STRING NOT NULL,
	DURATION INTERVAL NOT NULL,
  SPAN INT NOT NULL,
  MSG INT NOT NULL,
  TIMESTAMP TIMESTAMP NOT NULL,
  OPERATION STRING NOT NULL,
  LOG STRING NOT NULL
);
`,
	populate: func(p *planner, addRow func(...parser.Datum) error) error {
		trace := p.session.Tracing.trace
		if trace == nil {
			return nil
		}
		logs := make([]logRecordRow, 0)
		spans := trace.GetSpans()
		for i := range spans {
			spanWithIndex := spanWithIndex{
				RawSpan: &spans[i],
				index:   i,
			}
			for j, entry := range spans[i].Logs {
				lrr := logRecordRow{
					LogRecord: entry,
					span:      spanWithIndex,
					index:     j,
				}
				logs = append(logs, lrr)
			}
		}
		sort.Sort(byTimestamp(logs))

		var earliest time.Time
		if len(logs) > 0 {
			earliest = logs[0].Timestamp
		}

		for i, lrr := range logs {
			// Extract the message of the event, which is either in an "event" or
			// "error" field.
			var msg string
			for _, f := range lrr.Fields {
				key := f.Key()
				if key == "event" {
					msg = fmt.Sprint(f.Value())
					break
				}
				if key == "error" {
					msg = fmt.Sprint("error:", f.Value())
					break
				}
			}
			commulativeDuration := fmt.Sprintf("%.3fms", lrr.Timestamp.Sub(earliest).Seconds()*1000)
			var dur time.Duration
			if i > 0 {
				dur = lrr.Timestamp.Sub(logs[i-1].Timestamp)
			}
			err := addRow(
				parser.NewDString(commulativeDuration), // cummulative_duration
				&parser.DInterval{
					Duration: duration.Duration{
						Nanos: dur.Nanoseconds(),
					},
				}, // duration
				parser.NewDInt(parser.DInt(lrr.span.index)),           // span
				parser.NewDInt(parser.DInt(lrr.index)),                // msg
				parser.MakeDTimestamp(lrr.Timestamp, time.Nanosecond), // timestamp
				parser.NewDString(lrr.span.Operation),                 // operation
				parser.NewDString(msg))                                // log
			if err != nil {
				return err
			}
		}
		return nil
	},
}

type logRecordRow struct {
	opentracing.LogRecord
	span  spanWithIndex
	index int
}

type spanWithIndex struct {
	*basictracer.RawSpan
	index int
}

type byTimestamp []logRecordRow

func (a byTimestamp) Less(i, j int) bool {
	return a[i].Timestamp.Before(a[j].Timestamp)
}

func (a byTimestamp) Len() int {
	return len(a)
}

func (a byTimestamp) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
