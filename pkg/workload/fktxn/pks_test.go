// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	"math/rand"
	mathrandv2 "math/rand/v2"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestBuildPools_Shape verifies BuildPools produces one PK pool per table and
// one UC pool per non-PK unique constraint, each with the requested size and
// per-column tuples.
func TestBuildPools_Shape(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Schema with a composite PK (parent) and a non-PK unique constraint
	// (child) so we exercise both pool types.
	const ddl = `
		CREATE TABLE parent (
			a INT NOT NULL,
			b INT NOT NULL,
			PRIMARY KEY (a, b));
		CREATE TABLE child (
			id INT PRIMARY KEY,
			tag INT NOT NULL,
			UNIQUE (tag))`

	schema := discoverSchemaFromDDL(t, srv, sqlDB, "pools_shape", ddl)

	const poolSize = 5
	rng := rand.New(rand.NewSource(1))
	pools, err := BuildPools(rng, schema, poolSize)
	require.NoError(t, err)

	require.Len(t, pools.PKs, 2, "one PK pool per table")
	require.Len(t, pools.PKs["parent"], poolSize)
	require.Len(t, pools.PKs["child"], poolSize)
	for _, tuple := range pools.PKs["parent"] {
		require.Len(t, tuple, 2, "parent PK is composite (a, b)")
	}
	for _, tuple := range pools.PKs["child"] {
		require.Len(t, tuple, 1, "child PK is single column")
	}

	// One UC pool for the non-PK UNIQUE on child(tag); none on parent.
	require.Len(t, pools.UCs, 1)
	for _, tuples := range pools.UCs {
		require.Len(t, tuples, poolSize)
		for _, tuple := range tuples {
			require.Len(t, tuple, 1, "child tag UC is single column")
		}
	}
}

// TestPKValuesFor_Diamond_PropagatesParentPK verifies that for a child whose
// PK column is also an FK to a parent's PK, PKValuesFor reads the parent's
// pool value at the chain's pkIdx — keeping the FK target stable across the
// chain even though the child's own PK pool stores arbitrary independent
// values for that column.
func TestPKValuesFor_Diamond_PropagatesParentPK(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	const ddl = `
		CREATE TABLE parent (
			a INT NOT NULL,
			b INT NOT NULL,
			PRIMARY KEY (a, b));
		CREATE TABLE child (
			a INT NOT NULL,
			b INT NOT NULL,
			c INT NOT NULL,
			PRIMARY KEY (a, b, c),
			FOREIGN KEY (a, b) REFERENCES parent(a, b))`

	schema := discoverSchemaFromDDL(t, srv, sqlDB, "diamond_propagate", ddl)
	graphs := BuildFKGraphs(schema)
	require.Len(t, graphs, 1)
	sorted, sub, _, err := RandomSubDAG(mathrandv2.New(mathrandv2.NewPCG(2, 0)), graphs[0])
	require.NoError(t, err)

	const poolSize = 3
	pools, err := BuildPools(rand.New(rand.NewSource(1)), schema, poolSize)
	require.NoError(t, err)

	// For every pkIdx, the child's (a, b) PK columns must equal the parent's
	// (a, b) at that index.
	var parentTbl, childTbl *Table
	for _, t := range sorted {
		switch t.Name {
		case "parent":
			parentTbl = t
		case "child":
			childTbl = t
		}
	}
	require.NotNil(t, parentTbl)
	require.NotNil(t, childTbl)

	for pkIdx := 0; pkIdx < poolSize; pkIdx++ {
		parentPK, err := PKValuesFor(parentTbl, sub, pools, pkIdx)
		require.NoError(t, err)
		childPK, err := PKValuesFor(childTbl, sub, pools, pkIdx)
		require.NoError(t, err)
		require.Equal(t, parentPK[0], childPK[0], "child.a should equal parent.a at pkIdx=%d", pkIdx)
		require.Equal(t, parentPK[1], childPK[1], "child.b should equal parent.b at pkIdx=%d", pkIdx)
		// child.c is the child-only PK column; not propagated.
	}
}

// TestAssignPKs_NilPoolReturnsZero verifies the nil-pool fallback: AssignPKs
// returns 0 (no pool to index) so the caller's row-build path takes its own
// fallback to type-domain sampling.
func TestAssignPKs_NilPoolReturnsZero(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng := rand.New(rand.NewSource(1))
	require.Equal(t, 0, AssignPKs(rng, nil))
}

// TestAssignPKs_PoolReturnsInRangeIndex verifies AssignPKs returns a valid
// index into the pool — < poolSize — when a pool is supplied.
func TestAssignPKs_PoolReturnsInRangeIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	schema := discoverSchemaFromDDL(t, srv, sqlDB, "assignpks_range", simplePairDDL)

	const poolSize = 4
	pools, err := BuildPools(rand.New(rand.NewSource(1)), schema, poolSize)
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 100; i++ {
		idx := AssignPKs(rng, pools)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, poolSize, "iter %d: pkIdx out of range", i)
	}
}
