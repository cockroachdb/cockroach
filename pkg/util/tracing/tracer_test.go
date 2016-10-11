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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
		Start:        timeutil.Now(),
		Duration:     15 * time.Millisecond,
		Tags:         make(map[string]interface{}),
		Logs: []opentracing.LogRecord{
			{
				Timestamp: timeutil.Now().Add(2 * time.Millisecond),
			},
			{
				Timestamp: timeutil.Now().Add(5 * time.Millisecond),
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
