// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"testing"

	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otelsdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestLogTags(t *testing.T) {
	tr := NewTracer()
	sr := tracetest.NewSpanRecorder()
	otelTr := otelsdk.NewTracerProvider(otelsdk.WithSpanProcessor(sr)).Tracer("test")
	tr.SetOpenTelemetryTracer(otelTr)

	l := logtags.SingleTagBuffer("tag1", "val1")
	l = l.Add("tag2", "val2")
	sp1 := tr.StartSpan("foo", WithForceRealSpan(), WithLogTags(l))
	sp1.SetVerbose(true)
	sp1.Finish()
	require.NoError(t, TestingCheckRecordedSpans(sp1.GetRecording(), `
		span: foo
			tags: _verbose=1 tag1=val1 tag2=val2
	`))
	{
		require.Len(t, sr.Ended(), 1)
		otelSpan := sr.Ended()[0]
		exp := []attribute.KeyValue{
			{Key: "tag1", Value: attribute.StringValue("val1")},
			{Key: "tag2", Value: attribute.StringValue("val2")},
		}
		require.Equal(t, exp, otelSpan.Attributes())
	}

	RegisterTagRemapping("tag1", "one")
	RegisterTagRemapping("tag2", "two")

	sp2 := tr.StartSpan("bar", WithForceRealSpan(), WithLogTags(l))
	sp2.SetVerbose(true)
	sp2.Finish()
	require.NoError(t, TestingCheckRecordedSpans(sp2.GetRecording(), `
		span: bar
			tags: _verbose=1 one=val1 two=val2
	`))

	{
		require.Len(t, sr.Ended(), 2)
		otelSpan := sr.Ended()[1]
		exp := []attribute.KeyValue{
			{Key: "one", Value: attribute.StringValue("val1")},
			{Key: "two", Value: attribute.StringValue("val2")},
		}
		require.Equal(t, exp, otelSpan.Attributes())
	}

	sp3 := tr.StartSpan("baz", WithLogTags(l), WithForceRealSpan())
	sp3.SetVerbose(true)
	sp3.Finish()
	require.NoError(t, TestingCheckRecordedSpans(sp3.GetRecording(), `
		span: baz
			tags: _verbose=1 one=val1 two=val2
	`))
	{
		require.Len(t, sr.Ended(), 3)
		otelSpan := sr.Ended()[2]
		exp := []attribute.KeyValue{
			{Key: "one", Value: attribute.StringValue("val1")},
			{Key: "two", Value: attribute.StringValue("val2")},
		}
		require.Equal(t, exp, otelSpan.Attributes())
	}
}
