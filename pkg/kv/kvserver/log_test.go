// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	_ "github.com/lib/pq"
)

func TestLogSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	ts := s.(*server.TestServer)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	countSplits := func() int {
		var count int
		err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM system.rangelog WHERE "eventType" = $1`,
			kvserverpb.RangeLogEventType_split.String()).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		return count
	}

	// Count the number of split events.
	initialSplits := countSplits()

	// Generate an explicit split event.
	if err := kvDB.AdminSplit(ctx, "splitkey", hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	// Verify that the count has increased by at least one. Realistically it's
	// almost always by exactly one, but if there are any other splits they
	// might race in after the previous call to countSplits().
	if now := countSplits(); now <= initialSplits {
		t.Fatalf("expected >= %d splits, found %d", initialSplits, now)
	}

	// verify that RangeID always increases (a good way to see that the splits
	// are logged correctly)
	rows, err := db.QueryContext(ctx,
		`SELECT "rangeID", "otherRangeID", info FROM system.rangelog WHERE "eventType" = $1`,
		kvserverpb.RangeLogEventType_split.String(),
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
		var info kvserverpb.RangeLogEvent_Info
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for split of range %d: %+v", rangeID, err)
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

	store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
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
			`SELECT count(*) FROM system.rangelog WHERE ("uniqueID" & 0x7fff) = 0`).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("found %d uniqueIDs with a zero node ID", count)
		}
	}
}

func TestLogMerges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	ts := s.(*server.TestServer)
	store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}

	countRangeLogMerges := func() int {
		var count int
		err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM system.rangelog WHERE "eventType" = $1`,
			kvserverpb.RangeLogEventType_merge.String()).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		return count
	}

	// No ranges should have merged immediately after startup.
	if n := countRangeLogMerges(); n != 0 {
		t.Fatalf("expected 0 initial merges, but got %d", n)
	}
	if n := store.Metrics().RangeMerges.Count(); n != 0 {
		t.Errorf("expected 0 initial merges, but got %d", n)
	}

	// Create two ranges, then merge them.
	if err := kvDB.AdminSplit(ctx, "a", hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.AdminSplit(ctx, "b", hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.AdminMerge(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	if n := countRangeLogMerges(); n != 1 {
		t.Fatalf("expected 1 merge, but got %d", n)
	}
	if n := store.Metrics().RangeMerges.Count(); n != 1 {
		t.Errorf("expected 1 merge, but got %d", n)
	}

	rows, err := db.QueryContext(ctx,
		`SELECT "rangeID", "otherRangeID", info FROM system.rangelog WHERE "eventType" = $1`,
		kvserverpb.RangeLogEventType_merge.String(),
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
			t.Errorf("otherRangeID not recorded for merge of range %d", rangeID)
		}
		if otherRangeID.Int64 <= rangeID {
			t.Errorf("otherRangeID %d is not greater than rangeID %d", otherRangeID.Int64, rangeID)
		}
		if !infoStr.Valid {
			t.Errorf("info not recorded for merge of range %d", rangeID)
		}
		var info kvserverpb.RangeLogEvent_Info
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for merge of range %d: %+v", rangeID, err)
			continue
		}
		if int64(info.UpdatedDesc.RangeID) != rangeID {
			t.Errorf("recorded wrong updated descriptor %s for merge of range %d", info.UpdatedDesc, rangeID)
		}
		if int64(info.RemovedDesc.RangeID) != otherRangeID.Int64 {
			t.Errorf("recorded wrong new descriptor %s for merge of range %d", info.RemovedDesc, rangeID)
		}
	}
	if rows.Err() != nil {
		t.Fatal(rows.Err())
	}
}

func TestLogRebalances(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	logEvent := func(changeType roachpb.ReplicaChangeType, reason kvserverpb.RangeLogEventReason) {
		if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return store.LogReplicaChangeTest(ctx, txn, changeType, desc.InternalReplicas[0], *desc, reason, details)
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
	logEvent(roachpb.ADD_VOTER, kvserverpb.ReasonRangeUnderReplicated)
	checkMetrics(1 /*add*/, 0 /*remove*/)
	logEvent(roachpb.ADD_VOTER, kvserverpb.ReasonRangeUnderReplicated)
	checkMetrics(2 /*adds*/, 0 /*remove*/)
	logEvent(roachpb.REMOVE_VOTER, kvserverpb.ReasonRangeOverReplicated)
	checkMetrics(2 /*adds*/, 1 /*remove*/)

	// Open a SQL connection to verify that the events have been logged.
	pgURL, cleanupFn := sqlutils.PGUrl(t, s.ServingSQLAddr(), "TestLogRebalances", url.User(security.RootUser))
	defer cleanupFn()

	sqlDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()

	// verify that two add replica events have been logged.
	rows, err := sqlDB.QueryContext(ctx,
		`SELECT "rangeID", info FROM system.rangelog WHERE "eventType" = $1`,
		kvserverpb.RangeLogEventType_add_voter.String(),
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
		var info kvserverpb.RangeLogEvent_Info
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for add replica %d: %+v", rangeID, err)
			continue
		}
		if int64(info.UpdatedDesc.RangeID) != rangeID {
			t.Errorf("recorded wrong updated descriptor %s for add replica of range %d", info.UpdatedDesc, rangeID)
		}
		if a, e := *info.AddedReplica, desc.InternalReplicas[0]; a != e {
			t.Errorf("recorded wrong updated replica %s for add replica of range %d, expected %s",
				a, rangeID, e)
		}
		if a, e := info.Reason, kvserverpb.ReasonRangeUnderReplicated; a != e {
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
		`SELECT "rangeID", info FROM system.rangelog WHERE "eventType" = $1`,
		kvserverpb.RangeLogEventType_remove_voter.String(),
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
		var info kvserverpb.RangeLogEvent_Info
		if err := json.Unmarshal([]byte(infoStr.String), &info); err != nil {
			t.Errorf("error unmarshalling info string for remove replica %d: %+v", rangeID, err)
			continue
		}
		if int64(info.UpdatedDesc.RangeID) != rangeID {
			t.Errorf("recorded wrong updated descriptor %s for remove replica of range %d", info.UpdatedDesc, rangeID)
		}
		if a, e := *info.RemovedReplica, desc.InternalReplicas[0]; a != e {
			t.Errorf("recorded wrong updated replica %s for remove replica of range %d, expected %s",
				a, rangeID, e)
		}
		if a, e := info.Reason, kvserverpb.ReasonRangeOverReplicated; a != e {
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
