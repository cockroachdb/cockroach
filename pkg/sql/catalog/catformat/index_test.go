// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catformat

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestIndexForDisplay(t *testing.T) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()

	database := tree.Name("foo")
	table := tree.Name("bar")
	tableName := tree.MakeTableNameWithSchema(database, tree.PublicSchemaName, table)

	compExpr := "a + b"
	cols := []descpb.ColumnDescriptor{
		// a INT
		{
			ID:   1,
			Name: "a",
			Type: types.Int,
		},
		// b INT
		{
			ID:   2,
			Name: "b",
			Type: types.Int,
		},
		// c INT
		{
			ID:   3,
			Name: "c",
			Type: types.Int,
		},
		// d INT AS (a + b) VIRTUAL [INACCESSIBLE]
		{
			ID:           4,
			Name:         "d",
			Type:         types.Int,
			ComputeExpr:  &compExpr,
			Virtual:      true,
			Inaccessible: true,
		},
	}

	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name:    string(table),
		ID:      1,
		Columns: cols,
	}).BuildImmutableTable()

	// INDEX baz (a ASC, b DESC)
	baseIndex := descpb.IndexDescriptor{
		Name:                "baz",
		ID:                  0x0,
		KeyColumnNames:      []string{"a", "b"},
		KeyColumnIDs:        descpb.ColumnIDs{1, 2},
		KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_DESC},
	}

	// UNIQUE INDEX baz (a ASC, b DESC)
	uniqueIndex := baseIndex
	uniqueIndex.Unique = true

	// INVERTED INDEX baz (a)
	invertedIndex := baseIndex
	invertedIndex.Type = descpb.IndexDescriptor_INVERTED
	invertedIndex.KeyColumnNames = []string{"a"}
	invertedIndex.KeyColumnIDs = descpb.ColumnIDs{1}

	// INDEX baz (a ASC, b DESC) STORING (c)
	storingIndex := baseIndex
	storingIndex.StoreColumnNames = []string{"c"}

	// INDEX baz (a ASC, b DESC) WHERE a > 1:::INT8
	partialIndex := baseIndex
	partialIndex.Predicate = "a > 1:::INT8"

	// INDEX baz (a ASC, (a + b) DESC, b ASC)
	expressionIndex := baseIndex
	expressionIndex.KeyColumnNames = []string{"a", "d", "b"}
	expressionIndex.KeyColumnIDs = descpb.ColumnIDs{1, 4, 2}
	expressionIndex.KeyColumnDirections = []descpb.IndexDescriptor_Direction{
		descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_DESC, descpb.IndexDescriptor_ASC,
	}

	testData := []struct {
		index      descpb.IndexDescriptor
		tableName  tree.TableName
		partition  string
		interleave string
		expected   string
	}{
		{baseIndex, descpb.AnonymousTable, "", "", "INDEX baz (a ASC, b DESC)"},
		{baseIndex, tableName, "", "", "INDEX baz ON foo.public.bar (a ASC, b DESC)"},
		{uniqueIndex, descpb.AnonymousTable, "", "", "UNIQUE INDEX baz (a ASC, b DESC)"},
		{invertedIndex, descpb.AnonymousTable, "", "", "INVERTED INDEX baz (a)"},
		{storingIndex, descpb.AnonymousTable, "", "", "INDEX baz (a ASC, b DESC) STORING (c)"},
		{partialIndex, descpb.AnonymousTable, "", "", "INDEX baz (a ASC, b DESC) WHERE a > 1:::INT8"},
		{expressionIndex, descpb.AnonymousTable, "", "", "INDEX baz (a ASC, (a + b) DESC, b ASC)"},
		{
			partialIndex,
			descpb.AnonymousTable,
			" PARTITION BY LIST (a) (PARTITION p VALUES IN (2))",
			"",
			"INDEX baz (a ASC, b DESC) PARTITION BY LIST (a) (PARTITION p VALUES IN (2)) WHERE a > 1:::INT8",
		},
		{
			partialIndex,
			descpb.AnonymousTable,
			"",
			" INTERLEAVE IN PARENT par (a)",
			"INDEX baz (a ASC, b DESC) INTERLEAVE IN PARENT par (a) WHERE a > 1:::INT8",
		},
		{
			partialIndex,
			descpb.AnonymousTable,
			" PARTITION BY LIST (a) (PARTITION p VALUES IN (2))",
			" INTERLEAVE IN PARENT par (a)",
			"INDEX baz (a ASC, b DESC) INTERLEAVE IN PARENT par (a) PARTITION BY LIST (a) (PARTITION p VALUES IN (2)) WHERE a > 1:::INT8",
		},
	}

	for testIdx, tc := range testData {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			got, err := indexForDisplay(
				ctx, tableDesc, &tc.tableName, &tc.index, false /* isPrimary */, tc.partition, tc.interleave, &semaCtx,
			)
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if got != tc.expected {
				t.Errorf("expected '%s', got '%s'", tc.expected, got)
			}
		})
	}
}
