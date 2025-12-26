// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type fakeDescriptor struct {
	lease.LeasedDescriptor
	expiration hlc.Timestamp
}

func (e *fakeDescriptor) Expiration(ctx context.Context) hlc.Timestamp {
	return e.expiration
}

func TestTombstoneDescriptorLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(nil)

	before := clock.Now()
	expiration := before.Add(1, 0)
	after := expiration.Add(1, 0)

	tu := &tombstoneUpdater{}
	require.False(t, tu.hasValidLease(ctx, before))

	tu.leased.descriptor = &fakeDescriptor{
		expiration: expiration,
	}

	require.True(t, tu.hasValidLease(ctx, before))
	require.False(t, tu.hasValidLease(ctx, expiration))
	require.False(t, tu.hasValidLease(ctx, after))
}

func TestTombstoneUpdaterSetsOriginID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This is a regression test for a bug in the tombstone updater. The
	// tombstone updater should always set the origin ID. Previously, it would
	// not set the origin id which caused logical replication to pick up the
	// tombstone update as a deletion event. This is undesirable because the
	// tombstone update case is only used when replicating deletes and if a
	// replicated write generates an LDR event, it leads to looping.

	// Start server with two databases
	ctx := context.Background()
	server, s, runners, dbNames := setupServerWithNumDBs(t, ctx, testClusterBaseClusterArgs, 1, 2)
	defer server.Stopper().Stop(ctx)

	// Create test table on both databases
	destRunner := runners[1]

	// Create a tombstone updater
	desc := desctestutils.TestingGetMutableExistingTableDescriptor(
		s.DB(), s.Codec(), dbNames[0], "tab")
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	tu := newTombstoneUpdater(s.Codec(), s.DB(), s.LeaseManager().(*lease.Manager), desc.GetID(), sd, s.ClusterSettings())
	defer tu.ReleaseLeases(ctx)

	// Set up 1 way logical replication. The replication stream is used to ensure
	// that the tombstone update will not be replicated as a deletion event.
	var jobID jobspb.JobID
	destRunner.QueryRow(t,
		"CREATE LOGICAL REPLICATION STREAM FROM TABLE a.tab ON $1 INTO TABLE b.tab",
		GetPGURLs(t, s, dbNames)[0].String()).Scan(&jobID)

	// Write the row to the destination
	destRunner.Exec(t, "INSERT INTO tab VALUES (1, 42)")

	row := tree.Datums{
		tree.NewDInt(tree.DInt(1)), // k
		tree.DNull,                 // v (deleted)
	}

	_, err := tu.updateTombstone(ctx, nil, s.Clock().Now(), row)
	require.NoError(t, err)

	config := s.ExecutorConfig().(sql.ExecutorConfig)
	err = sql.DescsTxn(ctx, &config, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		_, err = tu.updateTombstone(ctx, txn, s.Clock().Now(), row)
		return err
	})
	require.NoError(t, err)

	// Wait for replication to advance
	WaitUntilReplicatedTime(t, s.Clock().Now(), destRunner, jobID)

	// Verify the row still exists in the destination
	destRunner.CheckQueryResults(t, "SELECT pk, payload FROM tab", [][]string{
		{"1", "42"},
	})
}
