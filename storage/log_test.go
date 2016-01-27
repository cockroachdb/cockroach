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
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestLogSplits(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := server.StartTestServer(t)
	defer s.Stop()

	pgUrl, cleanupFn := sqlutils.PGUrl(t, s, security.RootUser, os.TempDir(), "TestLogSplits")
	defer cleanupFn()

	db, err := sql.Open("postgres", pgUrl.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	countSplits := func() int {
		var count int
		// TODO(mrtracy): this should be a parameterized query, but due to #3660
		// it does not work. This should be changed when #3660 is fixed.
		err := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM system.rangelog WHERE eventType = '%s'`, string(storage.RangeEventLogSplit))).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		return count
	}

	// Count the number of split events.
	initialSplits := server.ExpectedInitialRangeCount() - 1
	if a, e := countSplits(), initialSplits; a != e {
		t.Fatalf("expected %d initial splits, found %d", e, a)
	}

	// Generate an explicit split event.
	kvDB, err := s.OpenDBClient(security.NodeUser)
	if err != nil {
		t.Fatal(err)
	}
	if err := kvDB.AdminSplit("splitkey"); err != nil {
		t.Fatal(err)
	}

	// verify that every the count has increased by one.
	if a, e := countSplits(), initialSplits+1; a != e {
		t.Fatalf("expected %d splits, found %d", e, a)
	}

	// verify that RangeID always increases (a good way to see that the splits
	// are logged correctly)
	// TODO(mrtracy): Change to parameterized query when #3660 is fixed.
	rows, err := db.Query(fmt.Sprintf(`SELECT rangeID, otherRangeID, info FROM system.rangelog WHERE eventType = '%s'`, string(storage.RangeEventLogSplit)))
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var rangeID int64
		var otherRangeID sql.NullInt64
		var infoStr sql.NullString
		if err := rows.Scan(&rangeID, &otherRangeID, &infoStr); err != nil {
			t.Fatal(err)
		}

		if !otherRangeID.Valid {
			t.Errorf("otherRangeID not recorded for split of range %d", rangeID)
		}
		if otherRangeID.Int64 <= rangeID {
			t.Errorf("otherRangeID %d is not greater than rangeID %d", otherRangeID.Int64, rangeID)
		}
		// Verify that info returns a json struct.
		if !infoStr.Valid {
			t.Errorf("info not recorded for split of range %d", rangeID)
		}
		var info struct {
			UpdatedDesc roachpb.RangeDescriptor
			NewDesc     roachpb.RangeDescriptor
		}
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for split of range %d: %s", rangeID, err)
			continue
		}
		if int64(info.UpdatedDesc.RangeID) != rangeID {
			t.Errorf("recorded wrong updated descriptor %s for split of range %d", info.UpdatedDesc, rangeID)
		}
		if int64(info.NewDesc.RangeID) != otherRangeID.Int64 {
			t.Errorf("recorded wrong new descriptor %s for split of range %d", info.NewDesc, rangeID)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
}
