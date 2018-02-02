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

package storage_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"net/url"
	"testing"

	_ "github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	countSplits := func() int {
		var count int
		err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM system.public.rangelog WHERE "eventType" = $1`,
			storage.RangeLogEventType_split.String()).Scan(&count)
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
	if err := kvDB.AdminSplit(context.TODO(), "splitkey", "splitkey"); err != nil {
		t.Fatal(err)
	}

	// verify that every the count has increased by one.
	if a, e := countSplits(), initialRanges; a != e {
		t.Fatalf("expected %d splits, found %d", e, a)
	}

	// verify that RangeID always increases (a good way to see that the splits
	// are logged correctly)
	rows, err := db.QueryContext(ctx,
		`SELECT "rangeID", "otherRangeID", info FROM system.public.rangelog WHERE "eventType" = $1`,
		storage.RangeLogEventType_split.String(),
	)
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
		var info storage.RangeLogEvent_Info
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
	// Verify that the minimum number of splits has occurred. This is a min
	// instead of an exact number, because the number of splits seems to vary
	// between different runs of this test.
	if a := store.Metrics().RangeSplits.Count(); a < minSplits {
		t.Errorf("splits = %d < min %d", a, minSplits)
	}

	{
		// Verify that the uniqueIDs have non-zero node IDs. The "& 0x7fff" is
		// using internal knowledge of the structure of uniqueIDs that the node ID
		// is embedded in the lower 15 bits. See #17560.
		var count int
		err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM system.public.rangelog WHERE ("uniqueID" & 0x7fff) = 0`).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("found %d uniqueIDs with a zero node ID", count)
		}
	}
}

func TestLogRebalances(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Use a client to get the RangeDescriptor for the first range. We will use
	// this range's information to log fake rebalance events.
	desc := &roachpb.RangeDescriptor{}
	if err := db.GetProto(ctx, keys.RangeDescriptorKey(roachpb.RKeyMin), desc); err != nil {
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
	const details = "test"
	logEvent := func(changeType roachpb.ReplicaChangeType, reason storage.RangeLogEventReason) {
		if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			return store.LogReplicaChangeTest(ctx, txn, changeType, desc.Replicas[0], *desc, reason, details)
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
	logEvent(roachpb.ADD_REPLICA, storage.ReasonRangeUnderReplicated)
	checkMetrics(1 /*add*/, 0 /*remove*/)
	logEvent(roachpb.ADD_REPLICA, storage.ReasonRangeUnderReplicated)
	checkMetrics(2 /*adds*/, 0 /*remove*/)
	logEvent(roachpb.REMOVE_REPLICA, storage.ReasonRangeOverReplicated)
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
	rows, err := sqlDB.QueryContext(ctx,
		`SELECT "rangeID", info FROM system.public.rangelog WHERE "eventType" = $1`,
		storage.RangeLogEventType_add.String(),
	)
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
		var info storage.RangeLogEvent_Info
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for add replica %d: %s", rangeID, err)
			continue
		}
		if int64(info.UpdatedDesc.RangeID) != rangeID {
			t.Errorf("recorded wrong updated descriptor %s for add replica of range %d", info.UpdatedDesc, rangeID)
		}
		if a, e := *info.AddedReplica, desc.Replicas[0]; a != e {
			t.Errorf("recorded wrong updated replica %s for add replica of range %d, expected %s",
				a, rangeID, e)
		}
		if a, e := info.Reason, storage.ReasonRangeUnderReplicated; a != e {
			t.Errorf("recorded wrong reason %s for add replica of range %d, expected %s",
				a, rangeID, e)
		}
		if a, e := info.Details, details; a != e {
			t.Errorf("recorded wrong details %s for add replica of range %d, expected %s",
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
	rows, err = sqlDB.QueryContext(ctx,
		`SELECT "rangeID", info FROM system.public.rangelog WHERE "eventType" = $1`,
		storage.RangeLogEventType_remove.String(),
	)
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
		var info storage.RangeLogEvent_Info
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for remove replica %d: %s", rangeID, err)
			continue
		}
		if int64(info.UpdatedDesc.RangeID) != rangeID {
			t.Errorf("recorded wrong updated descriptor %s for remove replica of range %d", info.UpdatedDesc, rangeID)
		}
		if a, e := *info.RemovedReplica, desc.Replicas[0]; a != e {
			t.Errorf("recorded wrong updated replica %s for remove replica of range %d, expected %s",
				a, rangeID, e)
		}
		if a, e := info.Reason, storage.ReasonRangeOverReplicated; a != e {
			t.Errorf("recorded wrong reason %s for add replica of range %d, expected %s",
				a, rangeID, e)
		}
		if a, e := info.Details, details; a != e {
			t.Errorf("recorded wrong details %s for add replica of range %d, expected %s",
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
