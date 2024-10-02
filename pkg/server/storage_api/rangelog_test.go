// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAdminAPIRangeLogByRangeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		// Disable the default test tenant for now as this tests fails
		// with it enabled. Tracked with #81590.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(context.Background())

	rangeID := 654321
	testCases := []struct {
		rangeID  int
		hasLimit bool
		limit    int
		expected int
	}{
		{rangeID, true, 0, 2},
		{rangeID, true, -1, 2},
		{rangeID, true, 1, 1},
		{rangeID, false, 0, 2},
		// We'll create one event that has rangeID+1 as the otherRangeID.
		{rangeID + 1, false, 0, 1},
	}

	for _, otherRangeID := range []int{rangeID + 1, rangeID + 2} {
		if _, err := db.Exec(
			`INSERT INTO system.rangelog (
             timestamp, "rangeID", "otherRangeID", "storeID", "eventType"
           ) VALUES (
             now(), $1, $2, $3, $4
          )`,
			rangeID, otherRangeID,
			1, // storeID
			kvserverpb.RangeLogEventType_add_voter.String(),
		); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range testCases {
		url := fmt.Sprintf("rangelog/%d", tc.rangeID)
		if tc.hasLimit {
			url += fmt.Sprintf("?limit=%d", tc.limit)
		}
		t.Run(url, func(t *testing.T) {
			var resp serverpb.RangeLogResponse
			if err := srvtestutils.GetAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}

			if e, a := tc.expected, len(resp.Events); e != a {
				t.Fatalf("expected %d events, got %d", e, a)
			}

			for _, event := range resp.Events {
				expID := roachpb.RangeID(tc.rangeID)
				if event.Event.RangeID != expID && event.Event.OtherRangeID != expID {
					t.Errorf("expected rangeID or otherRangeID to be %d, got %d and r%d",
						expID, event.Event.RangeID, event.Event.OtherRangeID)
				}
			}
		})
	}
}

// Test the range log API when queries are not filtered by a range ID (like in
// TestAdminAPIRangeLogByRangeID).
func TestAdminAPIFullRangeLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			// Disable the default test tenant for now as this tests fails
			// with it enabled. Tracked with #81590.
			DefaultTestTenant: base.TODOTestTenantDisabled,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableSplitQueue: true,
				},
			},
		})
	defer s.Stopper().Stop(context.Background())

	// Insert something in the rangelog table, otherwise it's empty for new
	// clusters.
	rows, err := db.Query(`SELECT count(1) FROM system.rangelog`)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatal("missing row")
	}
	var cnt int
	if err := rows.Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("expected 0 rows in system.rangelog, found: %d", cnt)
	}
	const rangeID = 100
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(
			`INSERT INTO system.rangelog (
             timestamp, "rangeID", "storeID", "eventType"
           ) VALUES (now(), $1, 1, $2)`,
			rangeID,
			kvserverpb.RangeLogEventType_add_voter.String(),
		); err != nil {
			t.Fatal(err)
		}
	}
	expectedEvents := 10

	testCases := []struct {
		hasLimit bool
		limit    int
		expected int
	}{
		{false, 0, expectedEvents},
		{true, 0, expectedEvents},
		{true, -1, expectedEvents},
		{true, 1, 1},
	}

	for _, tc := range testCases {
		url := "rangelog"
		if tc.hasLimit {
			url += fmt.Sprintf("?limit=%d", tc.limit)
		}
		t.Run(url, func(t *testing.T) {
			var resp serverpb.RangeLogResponse
			if err := srvtestutils.GetAdminJSONProto(s, url, &resp); err != nil {
				t.Fatal(err)
			}
			events := resp.Events
			if e, a := tc.expected, len(events); e != a {
				var sb strings.Builder
				for _, ev := range events {
					sb.WriteString(ev.String() + "\n")
				}
				t.Fatalf("expected %d events, got %d:\n%s", e, a, sb.String())
			}
		})
	}
}
