// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/ldrrandgen"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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

func TestTombstoneUpdaterRandomTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, s, runners, dbNames := setupServerWithNumDBs(t, ctx, testClusterBaseClusterArgs, 1, 1)
	defer server.Stopper().Stop(ctx)

	runner := runners[0]
	dbName := dbNames[0]

	rng, _ := randutil.NewPseudoRand()
	tableName := "rand_table"

	stmt := tree.SerializeForDisplay(ldrrandgen.GenerateLDRTable(ctx, rng, tableName, true))
	t.Logf("Creating table with schema: %s", stmt)
	runner.Exec(t, stmt)

	desc := desctestutils.TestingGetMutableExistingTableDescriptor(
		s.DB(), s.Codec(), dbName, tableName)

	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	tu := newTombstoneUpdater(s.Codec(), s.DB(), s.LeaseManager().(*lease.Manager), desc.GetID(), sd, s.ClusterSettings())
	defer tu.ReleaseLeases(ctx)

	columnSchemas := sqlwriter.GetColumnSchema(desc)
	cols := make([]catalog.Column, len(columnSchemas))
	for i, cs := range columnSchemas {
		cols[i] = cs.Column
	}

	config := s.ExecutorConfig().(sql.ExecutorConfig)

	session, err := sqlwriter.NewInternalSession(ctx, s.InternalDB().(isql.DB), sd, s.ClusterSettings())
	require.NoError(t, err)
	defer session.Close(ctx)

	writer, err := sqlwriter.NewRowWriter(ctx, desc, session)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		row := generateRandomRow(rng, cols)
		before := s.Clock().Now()
		after := s.Clock().Now()

		err := sql.DescsTxn(ctx, &config, func(
			ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
		) error {
			_, err := tu.updateTombstone(ctx, txn, after, row)
			return err
		})
		require.NoError(t, err)

		stats, err := tu.updateTombstone(ctx, nil, before, row)
		require.NoError(t, err)
		require.Equal(t, int64(1), stats.kvWriteTooOld, "expected 1 kv put for tombstone update")

		// Use the table writer to try and insert the previous row using the
		// `before` timestamp. This should fail with LWW error because the
		// tombstone was written at a later timestamp. It may fail with integer out
		// of range error if the table has computed columns and the random datums
		// add to produces something out of range.
		err = session.Txn(ctx, func(ctx context.Context) error {
			return writer.InsertRow(ctx, before, row)
		})
		require.Error(t, err)
		if !(strings.Contains(err.Error(), "integer out of range") || isLwwLoser(err)) {
			t.Fatalf("expected LWW or integer out of range error, got: %v", err)
		}
	}
}

func generateRandomRow(rng *rand.Rand, cols []catalog.Column) []tree.Datum {
	row := make([]tree.Datum, 0, len(cols))
	for _, col := range cols {
		datum := randgen.RandDatum(rng, col.GetType(), col.IsNullable())
		row = append(row, datum)
	}
	return row
}
