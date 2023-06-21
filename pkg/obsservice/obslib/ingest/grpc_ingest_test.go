// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ingest

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/obsutil"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	logspb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	v12 "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	v1 "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	otel_res_pb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/resource/v1"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/google/uuid"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestGRPCIngest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterID := uuid.MustParse("44875af2-aea5-4965-8f9c-63fec244fd41")
	testResource := &otel_res_pb.Resource{
		Attributes: []*v12.KeyValue{
			{
				Key:   obspb.ClusterID,
				Value: &v12.AnyValue{Value: &v12.AnyValue_StringValue{StringValue: clusterID.String()}},
			},
		},
	}
	testTimeSource := timeutil.NewManualTime(time.Date(2023, 6, 26, 12, 1, 0, 0, time.UTC))

	datadriven.RunTest(t, "testdata/grpc_ingest", func(t *testing.T, d *datadriven.TestData) string {
		ctx := context.Background()
		testConsumer := obsutil.NewTestCaptureConsumer()
		e := MakeEventIngester(ctx, testConsumer, testTimeSource)

		req := newReqBuilder(testResource)
		for _, line := range strings.Split(d.Input, "\n") {
			fields := strings.Split(line, ",")
			require.Len(t, fields, 2)
			req.withLogEvent(fields[0], fields[1])
		}

		_, err := e.Export(ctx, req.build())
		require.NoError(t, err)

		var buf bytes.Buffer
		for _, event := range testConsumer.Events() {
			fmt.Fprintf(&buf, "%# v\n", pretty.Formatter(event))
		}
		return buf.String()
	})
}

// reqBuilder uses a builder pattern to incrementally build a
// logspb.ExportLogsServiceRequest with various LogRecord events
// of a given type.
//
// When you're finished building the request, use build() to
// finalize the request object.
type reqBuilder struct {
	// The request we're building.
	req *logspb.ExportLogsServiceRequest
	// Accumulated LogRecords, segmented by event type. We use
	// a struct slice here instead of a map to provide deterministic
	// ordering when iterating.
	scopeLogs []*scopeLogs
	// The last event timestamp, starting at a static value.
	// Each LogEvent added to the builder will have a timestamp
	// that increments from this timestamp. The original value is
	// static, meaning that timestamps will be deterministic for
	// each run so long as the order in which they're added is the
	// same.
	lastTimestamp int64
}

type scopeLogs struct {
	eventType string
	logs      []v1.LogRecord
}

func newReqBuilder(resource *otel_res_pb.Resource) *reqBuilder {
	return &reqBuilder{
		req: &logspb.ExportLogsServiceRequest{
			ResourceLogs: []*v1.ResourceLogs{
				{Resource: resource},
			},
		},
		scopeLogs:     make([]*scopeLogs, 0),
		lastTimestamp: time.Date(2023, 6, 26, 12, 0, 0, 0, time.UTC).UnixNano(),
	}
}

func (r *reqBuilder) withLogEvent(eventType string, data string) *reqBuilder {
	var sl *scopeLogs
	for _, s := range r.scopeLogs {
		if s.eventType == eventType {
			sl = s
			break
		}
	}
	if sl == nil {
		sl = &scopeLogs{
			eventType: eventType,
			logs:      make([]v1.LogRecord, 0),
		}
		r.scopeLogs = append(r.scopeLogs, sl)
	}
	timestamp := r.lastTimestamp + 10000000
	r.lastTimestamp = timestamp
	sl.logs = append(sl.logs, v1.LogRecord{
		TimeUnixNano: uint64(timestamp),
		Body:         &v12.AnyValue{Value: &v12.AnyValue_StringValue{StringValue: data}},
		Attributes: []*v12.KeyValue{{
			Key:   obspb.EventlogEventTypeAttribute,
			Value: &v12.AnyValue{Value: &v12.AnyValue_StringValue{StringValue: eventType}},
		}},
	})
	return r
}

func (r *reqBuilder) build() *logspb.ExportLogsServiceRequest {
	for _, sl := range r.scopeLogs {
		r.req.ResourceLogs[0].ScopeLogs = append(r.req.ResourceLogs[0].ScopeLogs, v1.ScopeLogs{
			Scope: &v12.InstrumentationScope{
				Name:    sl.eventType,
				Version: "1.0",
			},
			LogRecords: sl.logs,
		})
	}
	return r.req
}
