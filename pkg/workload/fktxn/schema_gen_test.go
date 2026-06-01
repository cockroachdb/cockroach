// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

// TestGenerateSchemaEmitsNoActionFKs runs the generator with a handful of
// seeds and checks every emitted FK uses NO ACTION on both ON DELETE and ON
// UPDATE. The workload's transaction generator does not branch on reference
// action type; CASCADE / SET NULL / SET DEFAULT would silently violate that
// invariant.
func TestGenerateSchemaEmitsNoActionFKs(t *testing.T) {
	for _, seed := range []int64{1, 7, 42, 100, 12345} {
		tables, fkStmts, ok := generateSchema(seed, 6, 0.4)
		if !ok {
			continue
		}
		require.NotEmpty(t, tables, "seed %d produced no tables", seed)

		for _, stmt := range fkStmts {
			alter, ok := stmt.(*tree.AlterTable)
			require.True(t, ok, "seed %d: expected *tree.AlterTable, got %T", seed, stmt)
			for _, cmd := range alter.Cmds {
				add, ok := cmd.(*tree.AlterTableAddConstraint)
				if !ok {
					continue
				}
				fk, ok := add.ConstraintDef.(*tree.ForeignKeyConstraintTableDef)
				if !ok {
					continue
				}
				require.Equal(t, tree.ReferenceAction(0), fk.Actions.Delete,
					"seed %d FK %s: ON DELETE must be NO ACTION", seed, fk.Name)
				require.Equal(t, tree.ReferenceAction(0), fk.Actions.Update,
					"seed %d FK %s: ON UPDATE must be NO ACTION", seed, fk.Name)
			}
		}
	}
}

// TestGenerateSchemaDeterministic verifies that the same seed produces the
// same set of table names and FK statements. The orchestrator and the
// PostLoad hook both call generateSchema (via ensureSchema) — and a sync.Once
// guards the in-process cache, but the determinism contract is what makes
// repro on a fresh process possible.
func TestGenerateSchemaDeterministic(t *testing.T) {
	tablesA, fksA, okA := generateSchema(99, 5, 0.4)
	tablesB, fksB, okB := generateSchema(99, 5, 0.4)
	require.Equal(t, okA, okB)
	if !okA {
		t.Skip("seed 99 produced computed columns")
	}
	require.Equal(t, len(tablesA), len(tablesB))
	for i := range tablesA {
		require.Equal(t, tablesA[i].Name, tablesB[i].Name)
		require.Equal(t, tablesA[i].Schema, tablesB[i].Schema)
	}
	require.Equal(t, len(fksA), len(fksB))
	for i := range fksA {
		require.Equal(t, fksA[i].String(), fksB[i].String())
	}
}

// TestGenerateSchemaDensityScales sanity-checks that higher --fk-density
// produces strictly more FK edges on average. The mutator's geometric
// short-prefix means a per-pair attempt may emit zero or more columns, but
// across enough seeds the mean count should monotonically increase with
// density.
func TestGenerateSchemaDensityScales(t *testing.T) {
	const trials = 20
	const numTables = 8

	count := func(density float64) int {
		var total int
		for seed := int64(0); seed < trials; seed++ {
			_, fks, ok := generateSchema(seed, numTables, density)
			if !ok {
				continue
			}
			total += len(fks)
		}
		return total
	}

	low := count(0.05)
	high := count(0.5)
	require.Greater(t, high, low,
		"density 0.5 should produce more FKs than density 0.05 (got %d vs %d)", high, low)
}
