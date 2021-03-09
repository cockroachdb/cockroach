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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func TestIndexForDisplay(t *testing.T) {
	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()

	database := tree.Name("foo")
	table := tree.Name("bar")
	tableName := tree.MakeTableNameWithSchema(database, tree.PublicSchemaName, table)

	colNames := []string{"a", "b"}
	tableDesc := testTableDesc(
		string(table),
		[]testCol{{colNames[0], types.Int}, {colNames[1], types.Int}},
		nil,
	)

	indexName := "baz"
	baseIndex := descpb.IndexDescriptor{
		Name:             indexName,
		ID:               0x0,
		ColumnNames:      colNames,
		ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_DESC},
	}

	uniqueIndex := baseIndex
	uniqueIndex.Unique = true

	invertedIndex := baseIndex
	invertedIndex.Type = descpb.IndexDescriptor_INVERTED
	invertedIndex.ColumnNames = []string{"a"}

	storingIndex := baseIndex
	storingIndex.StoreColumnNames = []string{"c"}

	partialIndex := baseIndex
	partialIndex.Predicate = "a > 1:::INT8"

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
			got, err := IndexForDisplay(
				ctx, tableDesc, &tc.tableName, &tc.index, tc.partition, tc.interleave, &semaCtx,
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
