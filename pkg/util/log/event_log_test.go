// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base/serverident"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type testRegistry struct {
	serverSinks map[serverident.ServerIdentificationPayload]log.EventLogWriter
}

func (t *testRegistry) RegisterWriter(
	ctx context.Context, serverId serverident.ServerIdentifier, sink log.EventLogWriter,
) {

	t.serverSinks[serverId.GetServerIdentificationPayload()] = sink
}

func (t *testRegistry) RemoveWriter(serverId serverident.ServerIdentifier) {
	delete(t.serverSinks, serverId.GetServerIdentificationPayload())
}

func (t *testRegistry) GetWriter(serverId serverident.ServerIdentifier) (log.EventLogWriter, bool) {
	sink, ok := t.serverSinks[serverId.GetServerIdentificationPayload()]
	return sink, ok
}

type testWriter struct {
	capturedEvents []logpb.EventPayload
}

func (t *testWriter) Write(ctx context.Context, ev logpb.EventPayload, writeAsync bool) {
	t.capturedEvents = append(t.capturedEvents, ev)
}

type TestEventSev struct {
	event    logtestutils.TestEvent
	severity logpb.Severity
}

var _ logpb.EventPayload = &logtestutils.TestEvent{}
var _ log.EventLogWriterRegistry = &testRegistry{}
var _ log.EventLogWriter = &testWriter{}

func TestEventLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	registry := &testRegistry{serverSinks: make(map[serverident.ServerIdentificationPayload]log.EventLogWriter)}
	log.InitEventLogWriterRegistry(registry)
	ac := log.MakeTestingAmbientContext(nil)
	ctx := ac.AnnotateCtx(context.Background())
	writer := testWriter{}
	log.RegisterEventLogWriter(ctx, &ac, &writer)
	spy := logtestutils.NewStructuredLogSpy(
		t,
		[]logpb.Channel{logpb.Channel_DEV},
		[]string{logtestutils.TestEventType},
		func(entry logpb.Entry) (TestEventSev, error) {
			te, err := logtestutils.FromLogEntry[logtestutils.TestEvent](entry)
			return TestEventSev{event: te, severity: entry.Severity}, err
		},
	)

	cleanupTxnSpy := log.InterceptWith(ctx, spy)
	defer cleanupTxnSpy()

	t1 := logtestutils.TestEvent{Timestamp: timeutil.Now().UnixNano()}
	t2 := logtestutils.TestEvent{Timestamp: timeutil.Now().Add(time.Minute).UnixNano()}
	t3 := logtestutils.TestEvent{Timestamp: timeutil.Now().Add(time.Minute * 2).UnixNano()}
	log.EventLog(ctx, &ac, t1)
	log.EventLog(ctx, &ac, t2)
	log.EventLog(ctx, &ac, t3, log.WithWarning())

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

	// Check writer
	require.Equal(t, writer.capturedEvents, []logpb.EventPayload{t1, t2, t3})
}
