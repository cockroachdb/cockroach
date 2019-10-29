// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptverifier"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestProtectedTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)
	s0 := tc.Server(0)

	conn := tc.ServerConn(0)
	_, err := conn.Exec("CREATE TABLE foo (k INT PRIMARY KEY, v BYTES)")
	require.NoError(t, err)

	_, err = conn.Exec("SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms';")
	require.NoError(t, err)

	_, err = conn.Exec("ALTER TABLE foo CONFIGURE ZONE USING gc.ttlseconds = 1;")
	require.NoError(t, err)

	rRand, _ := randutil.NewPseudoRand()
	for {
		_, err := conn.Exec("UPSERT INTO foo VALUES (1, $1)", randutil.RandBytes(rRand, 1<<19))
		if testutils.IsError(err, "backpressure") {
			break
		}
		require.NoError(t, err)
	}

	row := conn.QueryRow("SELECT start_key FROM crdb_internal.ranges_no_leases WHERE table_name = $1 AND database_name = current_database() ORDER BY start_key ASC LIMIT 1", "foo")

	var startKey roachpb.Key
	require.NoError(t, row.Scan(&startKey))

	// Okay great now we have a key and can go find replicas and stores and what not.
	r := tc.LookupRangeOrFatal(t, startKey)
	l, _, err := tc.FindRangeLease(r, nil)
	require.NoError(t, err)

	lhServer := tc.Server(int(l.Replica.NodeID) - 1)
	s, repl := getFirstStoreReplica(t, lhServer, startKey)
	beforeGC := s.Clock().Now()
	trace, _, err := s.ManuallyEnqueue(ctx, "gc", repl, false)

	// So now we should have run GC.
	require.Regexp(t, "(?s)shouldQueue=true.*processing replica.*GC score after GC", trace.String())
	p := protectedts.WithDatabase(ptstorage.NewProvider(s0.ClusterSettings()), s.DB())
	ptsRec := ptpb.NewRecord(s0.Clock().Now(), ptpb.PROTECT_AT, "", nil, roachpb.Span{
		Key:    startKey,
		EndKey: startKey.Next(),
	})
	require.NoError(t, p.Protect(ctx, nil /* txn */, &ptsRec))

	for {
		_, err := conn.Exec("UPSERT INTO foo VALUES (1, $1)", randutil.RandBytes(rRand, 1<<19))
		if testutils.IsError(err, "backpressure") {
			break
		}
		require.NoError(t, err)
	}

	trace, _, err = s.ManuallyEnqueue(ctx, "gc", repl, false)
	require.NoError(t, err)
	require.Regexp(t, "(?s)not gc'ing replica.*due to protected timestamps.*"+ptsRec.ID.String(), trace.String())

	// Make a new record that is doomed to fail.
	failedRec := ptsRec
	failedRec.ID = uuid.MakeV4()
	failedRec.Timestamp = beforeGC
	require.NoError(t, p.Protect(ctx, nil /* txn */, &failedRec))
	failed, createdAt, err := p.GetRecord(ctx, nil /* txn */, failedRec.ID)
	require.NoError(t, err)
	// Verify that it indeed did fail.
	verifyErr := ptverifier.Verify(ctx, s0.DistSenderI().(*kv.DistSender), failed, createdAt)
	require.True(t, testutils.IsError(verifyErr, "failed to verify protection on"), "%v", err)
}
