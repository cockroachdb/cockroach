// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

const TestEventType = "test_event"

type TestRegistry struct {
	serverSinks map[serverident.ServerIdentificationPayload]log.SEventSink
}

func (t *TestRegistry) RegisterSink(
	ctx context.Context, ambientContext log.AmbientContext, sink log.SEventSink,
) {
	t.serverSinks[ambientContext.ServerIDs] = sink
}

func (t *TestRegistry) DeregisterSink(ambientContext log.AmbientContext) {
	delete(t.serverSinks, ambientContext.ServerIDs)
}

func (t *TestRegistry) GetSink(ambientContext log.AmbientContext) (log.SEventSink, bool) {
	sink, ok := t.serverSinks[ambientContext.ServerIDs]
	return sink, ok
}

type TestSink struct {
	capturedEvents []logpb.EventPayload
}

func (t *TestSink) WriteEvent(ctx context.Context, ev logpb.EventPayload, writeAsync bool) {
	t.capturedEvents = append(t.capturedEvents, ev)
}

type TestEvent struct {
	Timestamp int64 `json:"timestamp"`
}

type TestEventSev struct {
	event    TestEvent
	severity logpb.Severity
}

func (t TestEvent) CommonDetails() *logpb.CommonEventDetails {
	return &logpb.CommonEventDetails{
		Timestamp: t.Timestamp,
		EventType: TestEventType,
	}
}

func (t TestEvent) LoggingChannel() logpb.Channel {
	return logpb.Channel_DEV
}

func (t TestEvent) AppendJSONFields(
	printComma bool, b redact.RedactableBytes,
) (bool, redact.RedactableBytes) {
	return t.CommonDetails().AppendJSONFields(printComma, b)
}

var _ logpb.EventPayload = &TestEvent{}
var _ log.SEventSinkRegistry = &TestRegistry{}
var _ log.SEventSink = &TestSink{}

func TestStructuredEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	registry := &TestRegistry{serverSinks: make(map[serverident.ServerIdentificationPayload]log.SEventSink)}
	log.InitSink(registry)
	ac := log.MakeTestingAmbientContext(nil)
	ctx := ac.AnnotateCtx(context.Background())
	sink := TestSink{}
	log.RegisterEventLogSink(ctx, ac, &sink)
	spy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_DEV},
		[]string{TestEventType},
		func(entry logpb.Entry) (TestEventSev, error) {
			var structuredPayload TestEvent
			err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &structuredPayload)
			if err != nil {
				return TestEventSev{event: structuredPayload, severity: entry.Severity}, err
			}
			return TestEventSev{event: structuredPayload, severity: entry.Severity}, nil
		},
	)

	cleanupTxnSpy := log.InterceptWith(ctx, spy)
	defer cleanupTxnSpy()

	logger := log.NewSEventLogger(ac)

	t1 := TestEvent{Timestamp: timeutil.Now().UnixNano()}
	t2 := TestEvent{Timestamp: timeutil.Now().Add(time.Minute).UnixNano()}
	t3 := TestEvent{Timestamp: timeutil.Now().Add(time.Minute * 2).UnixNano()}
	logger.StructuredEvent(ctx, t1)
	logger.StructuredEvent(ctx, t2, log.WithWriteToTable(true))
	logger.StructuredEvent(ctx, t3, log.WithWriteToTable(true), log.WithWarning())

	capturedLogs := spy.GetLogs(logpb.Channel_DEV)
	require.Len(t, capturedLogs, 3)
	// check t1
	require.Equal(t, t1, capturedLogs[0].event)
	require.Equal(t, severity.INFO, capturedLogs[0].severity)

	// check t2
	require.Equal(t, t2, capturedLogs[1].event)
	require.Equal(t, severity.INFO, capturedLogs[1].severity)

	// check t3
	require.Equal(t, t3, capturedLogs[2].event)
	require.Equal(t, severity.WARNING, capturedLogs[2].severity)

	// Check sink
	require.Contains(t, sink.capturedEvents, t2)
}
