// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type testLogSink struct {
	server  *TestServer
	db      *sql.DB
	events  []string
	haveErr bool
}

var _ eventLogSink = &testLogSink{}

func (t *testLogSink) ReportEvent(
	ctx context.Context, ts time.Time, event string, reporter string, info string,
) {
	t.events = append(t.events, event)
}

func (t *testLogSink) Errorf(ctx context.Context, format string, args ...interface{}) {
	t.haveErr = true
}

func (t *testLogSink) doSink(ts time.Time) (time.Time, error) {
	t.events = nil
	t.haveErr = false
	return sinkEventlog(context.Background(),
		t, t.server.db, t.server.internalExecutor, &t.server.ClusterSettings().SV, ts)
}

func addEvents(t *testing.T, sqlDB *sqlutils.SQLRunner, startTs time.Time, num int) time.Time {
	for i := 0; i < num; i++ {
		startTs = startTs.Add(time.Second)
		sqlDB.Exec(t,
			`INSERT INTO system.eventlog (
					"timestamp", "eventType", "targetID", "reportingID"
					) VALUES ($1, $2, $3, $4)`,
			startTs, fmt.Sprintf("test_event-%d", i), i, i,
		)
	}
	return startTs
}

func TestSystemEventLogSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(db)
	ts := s.(*TestServer)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	sink := &testLogSink{server: ts, db: db}

	// Write some events to the event table.
	startTs := timeutil.Now()
	expectedEndTs := addEvents(t, sqlDB, startTs, 10)

	// Scan all entries.
	_ = sqlDB.Exec(t, "set cluster setting server.eventlogsink.max_entries=-1")

	// Since 'set cluster setting'  also logs to the events table,
	// arrange only for 'test_event' events to be picked up.
	_ = sqlDB.Exec(t, "set cluster setting server.eventlogsink.include_events='test_event'")

	// Verify we have 10 events.
	endTs, err := sink.doSink(startTs)
	require.NoError(t, err)
	require.Equal(t, expectedEndTs, endTs)
	require.Equal(t, 10, len(sink.events))
	require.False(t, sink.haveErr)

	// Limit the number of events.
	_ = sqlDB.Exec(t, "set cluster setting server.eventlogsink.max_entries=3")

	// Verify we get 3 latest events and ignore 7 older ones.
	endTs, err = sink.doSink(startTs)
	require.NoError(t, err)
	require.Equal(t, expectedEndTs, endTs)
	require.False(t, sink.haveErr)
	require.Equal(t, []string{"'test_event-7'", "'test_event-8'", "'test_event-9'"}, sink.events)

	// Exclude test_event 7 & 9
	_ = sqlDB.Exec(t, "set cluster setting server.eventlogsink.exclude_events='test_event-(7|9)'")
	endTs, err = sink.doSink(startTs)
	require.NoError(t, err)
	require.Equal(t, []string{"'test_event-5'", "'test_event-6'", "'test_event-8'"}, sink.events)
	require.Equal(t, startTs.Add(9*time.Second), endTs)
	require.False(t, sink.haveErr)

	// Query events starting from the previous endTs -- nothing should be returned.
	oldEndTs := endTs
	endTs, err = sink.doSink(oldEndTs)
	require.NoError(t, err)
	require.EqualValues(t, oldEndTs, endTs)
	require.Nil(t, sink.events)

	// Add a new event and make sure it is retrieved.
	expectedEndTs = addEvents(t, sqlDB, endTs, 1)
	endTs, err = sink.doSink(endTs)
	require.NoError(t, err)
	require.EqualValues(t, []string{"'test_event-0'"}, sink.events)
}
