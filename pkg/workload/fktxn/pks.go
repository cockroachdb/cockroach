// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	randworkload "github.com/cockroachdb/cockroach/pkg/workload/rand"
	"github.com/cockroachdb/errors"
)

// Pools holds run-stable, per-constraint value pools for the workload. PKs
// holds N PK tuples per table; UCs holds N tuples per non-PK unique
// constraint. Pools are independent: a column shared between a PK and a UC
// gets unrelated values in the two pools — there is no implication that "this
// PK has this UC". Cross-table coordination of FK columns happens at row-
// build time by reading FK column values from the *referenced* constraint's
// pool, not from the child's PK pool.
//
// Built once at startup over the whole schema (see BuildPools); rotation only
// re-picks the sub-DAG, not the keyspace. Run-stable pools mean cross-
// rotation rows have predictable identities so collisions resolve via
// UPSERT-as-update instead of UC violations.
type Pools struct {
	// PKs maps table name → N PK tuples for that table. Each tuple is a
	// []interface{} of length len(primaryKeyColumns(table)) in PK column order.
	PKs map[string][][]interface{}
	// UCs maps non-PK unique constraint name → N UC tuples. Each tuple is a
	// []interface{} of length len(uc.Columns) in UC column order. Primary key
	// constraints are not stored here (use PKs).
	UCs map[string][][]interface{}
}

// BuildPools pre-generates poolSize value tuples for every table's PK and
// every non-PK unique constraint across the whole schema. Pools are run-
// stable: callers build once at startup and reuse for the lifetime of the
// run, even across sub-DAG rotations. Returns an error if any constraint
// column lacks a discovered type.
//
// poolSize must be > 0; callers that want pool-free behavior should pass nil
// pools to AssignPKs / buildRow instead of building a zero-sized one.
func BuildPools(rng *rand.Rand, schema *Schema, poolSize int) (*Pools, error) {
	if poolSize <= 0 {
		return nil, errors.AssertionFailedf("BuildPools requires poolSize > 0, got %d", poolSize)
	}
	pools := &Pools{
		PKs: make(map[string][][]interface{}, len(schema.Tables)),
		UCs: make(map[string][][]interface{}),
	}
	// Walk tables in name order so pool generation is deterministic across
	// runs given the same RNG seed. Without this, Go's randomized map
	// iteration would produce different pool contents per run.
	tableNames := make([]string, 0, len(schema.Tables))
	for name := range schema.Tables {
		tableNames = append(tableNames, name)
	}
	sort.Strings(tableNames)
	for _, name := range tableNames {
		tbl := schema.Tables[name]
		pkCols := primaryKeyColumns(tbl)
		if len(pkCols) == 0 {
			return nil, errors.Newf("table %s has no primary key", tbl.Name)
		}
		colByName := make(map[string]Column, len(tbl.Columns))
		for _, c := range tbl.Columns {
			colByName[c.Name] = c
		}
		pkPool, err := buildPool(rng, tbl.Name, pkCols, colByName, poolSize)
		if err != nil {
			return nil, errors.Wrapf(err, "building PK pool for %s", tbl.Name)
		}
		pools.PKs[tbl.Name] = pkPool

		for _, uc := range tbl.UniqueConstraints {
			if uc.IsPrimary {
				continue
			}
			ucPool, err := buildPool(rng, tbl.Name, uc.Columns, colByName, poolSize)
			if err != nil {
				return nil, errors.Wrapf(err, "building UC pool for %s.%s", tbl.Name, uc.Name)
			}
			pools.UCs[uc.Name] = ucPool
		}
	}
	return pools, nil
}

// buildPool generates up to poolSize unique pool slots for one constraint.
// Each slot holds one value per column, in the column order given. Slots
// must be distinct row-wise (two slots are equal only if every column
// matches); a column value repeated across slots is fine as long as the
// rest of the slot differs.
//
// When the columns' joint domain is smaller than poolSize (e.g. a BOOL-only
// PK), the retry loop runs out before filling every slot. We accept the
// partial pool rather than failing — the workload still works, it just
// contends harder on a smaller keyspace.
func buildPool(
	rng *rand.Rand, tblName string, cols []string, colByName map[string]Column, poolSize int,
) ([][]interface{}, error) {
	const maxRetries = 100
	pool := make([][]interface{}, 0, poolSize)
	seen := make(map[string]bool, poolSize)
	for i := 0; i < poolSize; i++ {
		var slot []interface{}
		var key string
		fresh := false
		for attempt := 0; attempt < maxRetries; attempt++ {
			slot = make([]interface{}, len(cols))
			for j, name := range cols {
				col, ok := colByName[name]
				if !ok {
					return nil, errors.AssertionFailedf(
						"column %s.%s not found in column list", tblName, name,
					)
				}
				if col.Type == nil {
					return nil, errors.Newf("column %s.%s has no discovered type", tblName, name)
				}
				d := randgen.RandDatum(rng, col.Type, false /* nullOk */)
				v, err := randworkload.DatumToGoSQL(d)
				if err != nil {
					return nil, errors.Wrapf(err, "converting datum for %s.%s", tblName, name)
				}
				slot[j] = v
			}
			key = slotKey(slot)
			if !seen[key] {
				fresh = true
				break
			}
		}
		if !fresh {
			// Domain exhausted. Stop early with whatever we have; the workload
			// runs fine on a smaller pool, contention just gets sharper.
			break
		}
		seen[key] = true
		pool = append(pool, slot)
	}
	return pool, nil
}

// slotKey returns a stable string key for a pool slot's values, used to
// dedupe slots. fmt.Sprint on []interface{} produces a deterministic
// representation for the value types DatumToGoSQL emits.
func slotKey(slot []interface{}) string {
	return fmt.Sprint(slot)
}

// AssignPKs picks the chain's pkIdx — a single index into Pools.PKs[T] used
// for every table T in the sub-DAG for the lifetime of the chain. A single
// shared index keeps chain identity stable across UPSERT-as-update events:
// PK columns are read from PKs[T][pkIdx] every write, so the row's PK never
// drifts. FK columns that overlap PK columns are pinned to the same pkIdx
// against the parent's pool (see buildRow), which keeps the FK target stable
// too. Non-PK-overlapping FK columns and UC columns are re-rolled fresh per
// write — that's what produces real replicable diffs.
//
// We sample from the smallest table's pool size so the index is valid for
// every table. Pool sizes can differ across tables when a column type's
// domain is too small to fill the requested poolSize (e.g. a BOOL-only PK
// caps at 2 regardless of --row-pool-size).
//
// When pools is nil, returns 0 and the caller's row-build path falls back to
// type-domain sampling (the pre-pool behavior).
func AssignPKs(rng *rand.Rand, pools *Pools) int {
	if pools == nil {
		return 0
	}
	minSize := -1
	for _, tuples := range pools.PKs {
		if minSize == -1 || len(tuples) < minSize {
			minSize = len(tuples)
		}
	}
	if minSize <= 0 {
		return 0
	}
	return rng.Intn(minSize)
}

// PKValuesFor returns the PK column values for table t at the chain's pkIdx,
// with FK columns that overlap the PK overridden by the parent's PKs[pkIdx]
// values. The override is what stitches the chain together across tables: a
// child whose PK includes a column borrowed from a parent's PK (e.g. depts'
// org_id in the diamond schema) sees the parent's value at the same pkIdx,
// so the child row's PK is consistent with the FK target.
//
// Self-ref edges and edges to parents not in the sub-DAG are not overridden;
// the child's own PK pool value is used. dropped FK columns that participate
// in the PK are NOT overridden either — those columns are written as NULL by
// buildRow and patched by patchDroppedEdges.
//
// Used by ExecuteDelete and ExecuteUpdate when they need the row's PK in the
// WHERE clause. Returns nil if pools is nil; callers must have a non-nil
// pools to use this function.
func PKValuesFor(t *Table, sub *FKGraph, pools *Pools, pkIdx int) ([]interface{}, error) {
	if pools == nil {
		return nil, errors.AssertionFailedf("PKValuesFor requires non-nil pools")
	}
	pkTuples, ok := pools.PKs[t.Name]
	if !ok {
		return nil, errors.Newf("no PK pool for table %s", t.Name)
	}
	if pkIdx < 0 || pkIdx >= len(pkTuples) {
		return nil, errors.AssertionFailedf(
			"pkIdx %d out of range for %s (pool size %d)", pkIdx, t.Name, len(pkTuples),
		)
	}
	pkCols := primaryKeyColumns(t)
	out := make([]interface{}, len(pkCols))
	copy(out, pkTuples[pkIdx])

	subEdges := make(map[string]bool, len(sub.Edges))
	for _, e := range sub.Edges {
		subEdges[e.Name] = true
	}
	for i, pkCol := range pkCols {
		parent, parentCol, ok := lookupFKParent(t, pkCol, subEdges)
		if !ok {
			continue
		}
		parentTuples, ok := pools.PKs[parent]
		if !ok {
			continue
		}
		// Find the parent's PK column index so we can pull the right slot from
		// the parent tuple.
		parentTbl, ok := sub.Tables[parent]
		if !ok {
			continue
		}
		parentPKCols := primaryKeyColumns(parentTbl)
		for j, c := range parentPKCols {
			if c == parentCol {
				out[i] = parentTuples[pkIdx][j]
				break
			}
		}
	}
	return out, nil
}

// lookupFKParent returns the in-sub FK parent (if any) for childCol on tbl.
// Self-ref edges are skipped — they're handled via NULL + backfill, not via
// PK propagation.
func lookupFKParent(
	tbl *Table, childCol string, subEdges map[string]bool,
) (parent, parentCol string, ok bool) {
	for _, e := range tbl.OutboundFKs {
		if e.ReferencingTable == e.ReferencedTable {
			continue
		}
		if !subEdges[e.Name] {
			continue
		}
		for i, c := range e.ReferencingColumns {
			if c == childCol {
				return e.ReferencedTable, e.ReferencedColumns[i], true
			}
		}
	}
	return "", "", false
}
