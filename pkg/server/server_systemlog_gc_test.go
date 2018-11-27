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

package server

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage"

	"github.com/stretchr/testify/assert"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestLogGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ts := s.(*TestServer)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	const testRangeID = 10001
	const table = "rangelog"

	rangeLogRowCount := func() int {
		var count int
		err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM system.rangelog WHERE "rangeID" = $1`,
			testRangeID,
		).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		return count
	}

	logEvents := func(count int, timestamp time.Time) {
		for i := 0; i < count; i++ {
			_, err := db.Exec(
				`INSERT INTO system.rangelog (
             timestamp, "rangeID", "storeID", "eventType"
           ) VALUES (
             $1, $2, $3, $4
          )`,
				timestamp,
				testRangeID,
				1, // storeID
				storagepb.RangeLogEventType_add,
			)
			a.NoError(err)
		}
	}
	maxTS1 := timeutil.Now()
	maxTS2 := maxTS1.Add(time.Second)
	maxTS3 := maxTS2.Add(time.Second)
	maxTS4 := maxTS3.Add(time.Second)
	maxTS5 := maxTS4.Add(time.Hour)
	maxTS6 := maxTS5.Add(time.Hour)

	// Assert 0 rows before inserting any events.
	a.Equal(0, rangeLogRowCount())
	// Insert 100 events with timestamp of up to maxTS1.
	logEvents(100, maxTS1)
	a.Equal(100, rangeLogRowCount())
	// Insert 1 event with timestamp of up to maxTS2.
	logEvents(1, maxTS2)
	// Insert 49 event with timestamp of up to maxTS3.
	logEvents(49, maxTS3)
	a.Equal(150, rangeLogRowCount())
	// Insert 25 events with timestamp of up to maxTS4.
	logEvents(25, maxTS4)
	a.Equal(175, rangeLogRowCount())

	// GC up to maxTS1.
	tm, rowsGCd, err := ts.GCSystemLog(ctx, table, timeutil.Unix(0, 0), maxTS1)
	a.NoError(err)
	a.Equal(maxTS1, tm)
	a.True(rowsGCd >= 100, "Expected rowsGCd >= 100, found %d", rowsGCd)
	a.Equal(75, rangeLogRowCount())

	// GC exactly maxTS2.
	tm, rowsGCd, err = ts.GCSystemLog(ctx, table, maxTS2, maxTS2)
	a.NoError(err)
	a.Equal(maxTS2, tm)
	a.True(rowsGCd >= 1, "Expected rowsGCd >= 1, found %d", rowsGCd)
	a.Equal(74, rangeLogRowCount())

	// GC upto maxTS2.
	tm, rowsGCd, err = ts.GCSystemLog(ctx, table, maxTS1, maxTS3)
	a.NoError(err)
	a.Equal(maxTS3, tm)
	a.True(rowsGCd >= 49, "Expected rowsGCd >= 49, found %d", rowsGCd)
	a.Equal(25, rangeLogRowCount())
	// Insert 2000 more events.
	logEvents(2000, maxTS5)
	a.Equal(2025, rangeLogRowCount())

	// GC up to maxTS4.
	tm, rowsGCd, err = ts.GCSystemLog(ctx, table, maxTS2, maxTS4)
	a.NoError(err)
	a.Equal(maxTS4, tm)
	a.True(rowsGCd >= 25, "Expected rowsGCd >= 25, found %d", rowsGCd)
	a.Equal(2000, rangeLogRowCount())

	// GC everything.
	tm, rowsGCd, err = ts.GCSystemLog(ctx, table, maxTS4, maxTS5)
	a.NoError(err)
	a.Equal(maxTS5, tm)
	a.True(rowsGCd >= 2000, "Expected rowsGCd >= 2000, found %d", rowsGCd)
	a.Equal(0, rangeLogRowCount())

	// Ensure no errors when lowerBound > upperBound.
	logEvents(5, maxTS6)
	tm, rowsGCd, err = ts.GCSystemLog(ctx, table, maxTS6.Add(time.Hour), maxTS6)
	a.NoError(err)
	a.Equal(maxTS6, tm)
	a.Equal(int64(0), rowsGCd)
	a.Equal(5, rangeLogRowCount())
}

func TestLogGCTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	systemLogRowCount := func(ctx context.Context, db *gosql.DB, table string, ts time.Time) int {
		var count int
		err := db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT count(*) FROM system.%s WHERE timestamp <= $1`, table),
			ts,
		).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		return count
	}

	systemLogMaxTS := func(ctx context.Context, db *gosql.DB, table string) time.Time {
		var time time.Time
		err := db.QueryRowContext(ctx,
			fmt.Sprintf(`SELECT timestamp FROM system.%s ORDER by timestamp DESC LIMIT 1`, table),
		).Scan(&time)
		if err != nil {
			t.Fatal(err)
		}
		return time
	}

	testCases := []struct {
		table   string
		setting *settings.DurationSetting
	}{
		{
			table:   "rangelog",
			setting: rangeLogTTL,
		},
		{
			table:   "eventlog",
			setting: eventLogTTL,
		},
	}

	gcDone := make(chan struct{})
	storeKnobs := &storage.StoreTestingKnobs{
		SystemLogsGCGCDone: gcDone,
		SystemLogsGCPeriod: time.Nanosecond,
	}

	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: storeKnobs,
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	for _, tc := range testCases {
		t.Run(tc.table, func(t *testing.T) {
			a := assert.New(t)
			maxTS := systemLogMaxTS(ctx, db, tc.table)
			a.NotEqual(systemLogRowCount(ctx, db, tc.table, maxTS), 0, "Expected non zero number of rows before %v", maxTS)

			// Reading gcDone once ensures that the previous gc is done
			// (it could have been done long back and is waiting to send on this channel),
			// and the next gc has started.
			// Reading it twice guarantees that the next gc has also completed.
			// Before running the assertions below one gc run has to be guaranteed.
			<-gcDone
			<-gcDone
			a.NotEqual(
				systemLogRowCount(ctx, db, tc.table, maxTS),
				0,
				"Expected non zero number of events before %v as gc is not enabled",
				maxTS,
			)

			_, err := db.Exec(fmt.Sprintf("SET CLUSTER SETTING server.%s.ttl='1ns'", tc.table))
			a.NoError(err)
			time.Sleep(time.Second)

			<-gcDone
			<-gcDone
			a.Equal(0, systemLogRowCount(ctx, db, tc.table, maxTS), "Expected zero events before %v after gc", maxTS)
		})
	}
}
