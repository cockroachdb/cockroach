// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/ldrrandgen"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

func TestTombstoneUpdaterRandomTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	rng, _ := randutil.NewPseudoRand()
	tableName := "rand_table"

	writerType := sqlclustersettings.LDRWriterType(
		sqlclustersettings.LDRImmediateModeWriter.Default())
	stmt := tree.SerializeForDisplay(
		ldrrandgen.GenerateLDRTable(ctx, rng, tableName, writerType))
	t.Logf("Creating table with schema: %s", stmt)
	sqlDB.Exec(t, stmt)

	desc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), tree.Name(tableName))

	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	tu := NewTombstoneUpdater(
		s.Codec(), s.DB(), s.LeaseManager().(*lease.Manager),
		desc.GetID(), sd, s.ClusterSettings())
	defer tu.ReleaseLeases(ctx)

	columnSchemas := GetColumnSchema(desc)
	cols := make([]catalog.Column, len(columnSchemas))
	for i, cs := range columnSchemas {
		cols[i] = cs.Column
	}

	session := newInternalSession(t, s)
	defer session.Close(ctx)

	writer, err := NewRowWriter(ctx, desc, session)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		row := generateRandomRow(rng, cols)
		before := s.Clock().Now()
		after := s.Clock().Now()

		err := s.InternalDB().(isql.DB).Txn(ctx,
			func(ctx context.Context, txn isql.Txn) error {
				_, err := tu.UpdateTombstone(ctx, txn, after, row)
				return err
			})
		require.NoError(t, err)

		lwwLoss, err := tu.UpdateTombstone(ctx, nil, before, row)
		require.NoError(t, err)
		require.True(t, lwwLoss, "expected LWW loss for tombstone update")

		// Use the table writer to try and insert the previous row using the
		// `before` timestamp. This should fail with LWW error because the
		// tombstone was written at a later timestamp. It may fail with integer out
		// of range error if the table has computed columns and the random datums
		// add to produces something out of range.
		err = session.Txn(ctx, func(ctx context.Context) error {
			return writer.InsertRow(ctx, before, row)
		})
		require.Error(t, err)
		if !(strings.Contains(err.Error(), "integer out of range") ||
			IsLwwLoser(err)) {
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

func TestTombstoneDescriptorLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(nil)

	before := clock.Now()
	expiration := before.Add(1, 0)
	after := expiration.Add(1, 0)

	tu := &TombstoneUpdater{}
	require.False(t, tu.hasValidLease(ctx, before))

	tu.leased.descriptor = &fakeDescriptor{
		expiration: expiration,
	}

	require.True(t, tu.hasValidLease(ctx, before))
	require.False(t, tu.hasValidLease(ctx, expiration))
	require.False(t, tu.hasValidLease(ctx, after))
}
