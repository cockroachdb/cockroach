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
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"

	_ "github.com/lib/pq"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestLogSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

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
	initialRanges, err := server.ExpectedInitialRangeCount(kvDB)
	if err != nil {
		t.Fatal(err)
	}
	initialSplits := initialRanges - 1
	if a, e := countSplits(), initialSplits; a != e {
		t.Fatalf("expected %d initial splits, found %d", e, a)
	}

	// Generate an explicit split event.
	if err := kvDB.AdminSplit(context.TODO(), "splitkey"); err != nil {
		t.Fatal(err)
	}

	// verify that every the count has increased by one.
	if a, e := countSplits(), initialRanges; a != e {
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
		var otherRangeID gosql.NullInt64
		var infoStr gosql.NullString
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

	// This code assumes that there is only one TestServer, and thus that
	// StoreID 1 is present on the testserver. If this assumption changes in the
	// future, *any* store will work, but a new method will need to be added to
	// Stores (or a creative usage of VisitStores could suffice).
	store, pErr := s.(*server.TestServer).Stores().GetStore(roachpb.StoreID(1))
	if pErr != nil {
		t.Fatal(pErr)
	}
	minSplits := int64(initialSplits + 1)
	// Verify that the minimimum number of splits has occurred. This is a min
	// instead of an exact number, because the number of splits seems to vary
	// between different runs of this test.
	if a := store.Metrics().RangeSplits.Count(); a < minSplits {
		t.Errorf("splits = %d < min %d", a, minSplits)
	}
}

func TestLogRebalances(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop()

	// Use a client to get the RangeDescriptor for the first range. We will use
	// this range's information to log fake rebalance events.
	desc := &roachpb.RangeDescriptor{}
	if err := db.GetProto(context.TODO(), keys.RangeDescriptorKey(roachpb.RKeyMin), desc); err != nil {
		t.Fatal(err)
	}

	// This code assumes that there is only one TestServer, and thus that
	// StoreID 1 is present on the testserver. If this assumption changes in the
	// future, *any* store will work, but a new method will need to be added to
	// Stores (or a creative usage of VisitStores could suffice).
	store, err := s.(*server.TestServer).Stores().GetStore(roachpb.StoreID(1))
	if err != nil {
		t.Fatal(err)
	}

	// Log several fake events using the store.
	logEvent := func(changeType roachpb.ReplicaChangeType) {
		if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
			return store.LogReplicaChangeTest(ctx, txn, changeType, desc.Replicas[0], *desc)
		}); err != nil {
			t.Fatal(err)
		}
	}
	checkMetrics := func(expAdds, expRemoves int64) {
		if a, e := store.Metrics().RangeAdds.Count(), expAdds; a != e {
			t.Errorf("range adds %d != expected %d", a, e)
		}
		if a, e := store.Metrics().RangeRemoves.Count(), expRemoves; a != e {
			t.Errorf("range removes %d != expected %d", a, e)
		}
	}
	logEvent(roachpb.ADD_REPLICA)
	checkMetrics(1 /*add*/, 0 /*remove*/)
	logEvent(roachpb.ADD_REPLICA)
	checkMetrics(2 /*adds*/, 0 /*remove*/)
	logEvent(roachpb.REMOVE_REPLICA)
	checkMetrics(2 /*adds*/, 1 /*remove*/)

	// Open a SQL connection to verify that the events have been logged.
	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingAddr(), "TestLogRebalances", url.User(security.RootUser))
	defer cleanupFn()

	sqlDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	// verify that two add replica events have been logged.
	// TODO(mrtracy): placeholders still appear to be broken, this query should
	// be using a string placeholder for the eventType value.
	rows, err := sqlDB.Query(`SELECT rangeID, info FROM system.rangelog WHERE eventType = 'add'`)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for rows.Next() {
		count++
		var rangeID int64
		var infoStr gosql.NullString
		if err := rows.Scan(&rangeID, &infoStr); err != nil {
			t.Fatal(err)
		}

		if a, e := roachpb.RangeID(rangeID), desc.RangeID; a != e {
			t.Errorf("wrong rangeID %d recorded for add event, expected %d", a, e)
		}
		// Verify that info returns a json struct.
		if !infoStr.Valid {
			t.Errorf("info not recorded for add replica of range %d", rangeID)
		}
		var info struct {
			AddReplica  roachpb.ReplicaDescriptor
			UpdatedDesc roachpb.RangeDescriptor
		}
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for add replica %d: %s", rangeID, err)
			continue
		}
		if int64(info.UpdatedDesc.RangeID) != rangeID {
			t.Errorf("recorded wrong updated descriptor %s for add replica of range %d", info.UpdatedDesc, rangeID)
		}
		if a, e := info.AddReplica, desc.Replicas[0]; a != e {
			t.Errorf("recorded wrong updated replica %s for add replica of range %d, expected %s",
				a, rangeID, e)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if a, e := count, 2; a != e {
		t.Errorf("expected %d AddReplica events logged, found %d", e, a)
	}

	// verify that one remove replica event was logged.
	rows, err = sqlDB.Query(`SELECT rangeID, info FROM system.rangelog WHERE eventType = 'remove'`)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for rows.Next() {
		count++
		var rangeID int64
		var infoStr gosql.NullString
		if err := rows.Scan(&rangeID, &infoStr); err != nil {
			t.Fatal(err)
		}

		if a, e := roachpb.RangeID(rangeID), desc.RangeID; a != e {
			t.Errorf("wrong rangeID %d recorded for remove event, expected %d", a, e)
		}
		// Verify that info returns a json struct.
		if !infoStr.Valid {
			t.Errorf("info not recorded for remove replica of range %d", rangeID)
		}
		var info struct {
			RemovedReplica roachpb.ReplicaDescriptor
			UpdatedDesc    roachpb.RangeDescriptor
		}
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for remove replica %d: %s", rangeID, err)
			continue
		}
		if int64(info.UpdatedDesc.RangeID) != rangeID {
			t.Errorf("recorded wrong updated descriptor %s for remove replica of range %d", info.UpdatedDesc, rangeID)
		}
		if a, e := info.RemovedReplica, desc.Replicas[0]; a != e {
			t.Errorf("recorded wrong updated replica %s for remove replica of range %d, expected %s",
				a, rangeID, e)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
	if a, e := count, 1; a != e {
		t.Errorf("expected %d RemoveReplica events logged, found %d", e, a)
	}
}
