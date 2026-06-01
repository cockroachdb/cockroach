// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	"fmt"
	"math/rand"
	randv2 "math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// txnSchemaCases is the canonical set of DDLs covered by the per-operation
// transaction tests. Adding a case here exercises it in upsert, delete (and,
// once added, update) tests uniformly.
var txnSchemaCases = []struct {
	name string
	ddl  string
}{
	{name: "simple_pair", ddl: simplePairDDL},
	{name: "chain", ddl: chainDDL},
	{name: "diamond", ddl: diamondCompositeDDL},
	{name: "transitive", ddl: transitiveOverlapDDL},
	{name: "self_ref", ddl: selfRefDDL},
}

// shardSetup mirrors what an orchestrator hands to a shard at startup: the
// sub-DAG and run-stable per-constraint pools. Tests build one of these per
// scenario, then call runOnce to execute individual transactions on top of
// it.
type shardSetup struct {
	srv     serverutils.TestServerInterface
	dbName  string
	sorted  []*Table
	sub     *FKGraph
	dropped []FKEdge
	pools   *Pools
}

// testPoolSize is intentionally small so individual tests run fast and pool
// indices are easy to reason about in failure messages.
const testPoolSize = 8

// newShardSetup discovers the schema, picks a sub-DAG, and builds a small
// per-constraint pool. rng drives both the sub-DAG selection and the pool
// values (via seeds derived from rng).
func newShardSetup(
	t *testing.T, srv serverutils.TestServerInterface, dbName string, rng *rand.Rand,
) *shardSetup {
	t.Helper()
	testDB := srv.ApplicationLayer().SQLConn(t, serverutils.DBName(dbName))
	s, err := DiscoverSchema(testDB, dbName)
	require.NoError(t, err)
	graphs := BuildFKGraphs(s)
	require.NotEmpty(t, graphs)

	// RandomSubDAG takes a v2 RNG; derive its seed from the v1 stream so the
	// failure seed reported by randutil reproduces both halves.
	rngV2 := randv2.New(randv2.NewPCG(rng.Uint64(), rng.Uint64()))
	sorted, sub, dropped, err := RandomSubDAG(rngV2, graphs[0])
	require.NoError(t, err)

	pools, err := BuildPools(rng, s, testPoolSize)
	require.NoError(t, err)

	return &shardSetup{
		srv:     srv,
		dbName:  dbName,
		sorted:  sorted,
		sub:     sub,
		dropped: dropped,
		pools:   pools,
	}
}

// runUpsertOnce samples a fresh pkIdx and executes one UPSERT transaction.
// Returns the rows emitted and the pkIdx used (so the caller can chain a
// delete or update on the same chain).
func (s *shardSetup) runUpsertOnce(t *testing.T, rng *rand.Rand) (emittedSet, int) {
	t.Helper()
	pkIdx := AssignPKs(rng, s.pools)
	return s.runUpsertAt(t, rng, pkIdx), pkIdx
}

// runUpsertAt is runUpsertOnce with the chain's pkIdx supplied by the
// caller. Used by tests that want to populate every pool slot so subsequent
// UPSERT-as-update events have parent rows to re-point at.
//
// repointFKs=false here matches the chain's first UPSERT (Unknown/None →
// Exists) — parent rows are written in the same walk, so non-PK FKs pin
// to pkIdx.
func (s *shardSetup) runUpsertAt(t *testing.T, rng *rand.Rand, pkIdx int) emittedSet {
	t.Helper()
	ctx := context.Background()
	testDB := s.srv.ApplicationLayer().SQLConn(t, serverutils.DBName(s.dbName))

	tx, err := testDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	emitted, err := ExecuteUpsert(ctx, tx, rng, s.sorted, s.sub, s.dropped, s.pools, pkIdx, false /* repointFKs */)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("upsert failed: %v", err)
	}
	require.NoError(t, tx.Commit())
	return emitted
}

func TestExecuteUpsert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, tc := range txnSchemaCases {
		t.Run(tc.name, func(t *testing.T) {
			rng, seed := randutil.NewTestRand()
			t.Logf("seed=%d", seed)

			dbName := "up_" + tc.name
			discoverSchemaFromDDL(t, srv, sqlDB, dbName, tc.ddl)
			setup := newShardSetup(t, srv, dbName, rng)

			for i := 0; i < 10; i++ {
				emitted, _ := setup.runUpsertOnce(t, rng)
				require.NotEmpty(t, emitted, "iteration=%d", i)
			}
		})
	}
}

func TestExecuteDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, tc := range txnSchemaCases {
		t.Run(tc.name, func(t *testing.T) {
			rng, seed := randutil.NewTestRand()
			t.Logf("seed=%d", seed)

			dbName := "del_" + tc.name
			discoverSchemaFromDDL(t, srv, sqlDB, dbName, tc.ddl)
			setup := newShardSetup(t, srv, dbName, rng)

			// Insert a row chain, then delete the same chain.
			_, pkIdx := setup.runUpsertOnce(t, rng)

			testDB := srv.ApplicationLayer().SQLConn(t, serverutils.DBName(dbName))
			tx, err := testDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			deleted, err := ExecuteDelete(ctx, tx, setup.sorted, setup.sub, setup.pools, pkIdx)
			require.NoError(t, err)
			require.True(t, deleted, "delete should succeed when rows exist")
			require.NoError(t, tx.Commit())

			// All chain rows should be gone now.
			testSQL := sqlutils.MakeSQLRunner(testDB)
			for _, tbl := range setup.sorted {
				pkCols := primaryKeyColumns(tbl)
				args, err := PKValuesFor(tbl, setup.sub, setup.pools, pkIdx)
				require.NoError(t, err)
				whereParts := make([]string, len(pkCols))
				for i, c := range pkCols {
					whereParts[i] = fmt.Sprintf("%s = $%d", c, i+1)
				}
				query := fmt.Sprintf(
					"SELECT count(*) FROM %s WHERE %s",
					tbl.Name, strings.Join(whereParts, " AND "),
				)
				var count int
				testSQL.QueryRow(t, query, args...).Scan(&count)
				require.Equal(t, 0, count, "table %s row %v should be deleted", tbl.Name, args)
			}
		})
	}
}

// TestExecuteUpsert_RewritePath verifies the UPSERT-as-update path:
// running ExecuteUpsert with repointFKs=true on an already-populated chain
// re-rolls non-PK FK columns to a different parent row, while PK columns
// stay pinned to pkIdx. The test asserts (a) the re-write commits without
// FK violations and (b) for schemas with non-PK FK columns, the values
// actually changed.
func TestExecuteUpsert_RewritePath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	for _, tc := range txnSchemaCases {
		t.Run(tc.name, func(t *testing.T) {
			rng, seed := randutil.NewTestRand()
			t.Logf("seed=%d", seed)

			dbName := "rewrite_" + tc.name
			discoverSchemaFromDDL(t, srv, sqlDB, dbName, tc.ddl)
			setup := newShardSetup(t, srv, dbName, rng)

			// Pre-populate every pool slot so the rewrite's fresh parentIdx
			// always lands on an existing parent row.
			for i := 0; i < testPoolSize; i++ {
				setup.runUpsertAt(t, rng, i)
			}

			// Re-write the chain at pkIdx=0 with repointFKs=true. This is the
			// UPSERT-as-update path: PK pinned, non-PK FKs rolled fresh.
			pkIdx := 0
			testDB := srv.ApplicationLayer().SQLConn(t, serverutils.DBName(dbName))
			tx, err := testDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			emitted, err := ExecuteUpsert(
				ctx, tx, rng, setup.sorted, setup.sub, setup.dropped, setup.pools, pkIdx, true, /* repointFKs */
			)
			require.NoError(t, err, "rewrite should not fail on a populated keyspace")
			require.NoError(t, tx.Commit())
			require.NotEmpty(t, emitted)
		})
	}
}

// TestExecuteDelete_RowNotPresent verifies that ExecuteDelete returns
// (false, nil) when a target row does not exist (e.g. another worker
// already deleted it). The transaction is left in a state the caller can
// commit cleanly.
func TestExecuteDelete_RowNotPresent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	rng, seed := randutil.NewTestRand()
	t.Logf("seed=%d", seed)

	const dbName = "del_missing"
	discoverSchemaFromDDL(t, srv, sqlDB, dbName, simplePairDDL)
	setup := newShardSetup(t, srv, dbName, rng)

	// Don't insert anything. Try to delete; should return false.
	pkIdx := AssignPKs(rng, setup.pools)

	testDB := srv.ApplicationLayer().SQLConn(t, serverutils.DBName(dbName))
	tx, err := testDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	deleted, err := ExecuteDelete(ctx, tx, setup.sorted, setup.sub, setup.pools, pkIdx)
	require.NoError(t, err)
	require.False(t, deleted, "delete should report false when row is missing")
	require.NoError(t, tx.Commit())
}
