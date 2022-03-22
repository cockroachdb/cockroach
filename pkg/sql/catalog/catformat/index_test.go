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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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

	// JSONB INVERTED INDEX baz (a)
	jsonbInvertedIndex := baseIndex
	jsonbInvertedIndex.Type = descpb.IndexDescriptor_INVERTED
	jsonbInvertedIndex.KeyColumnNames = []string{"a"}
	jsonbInvertedIndex.KeyColumnIDs = descpb.ColumnIDs{1}

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

	// Hash Sharded INDEX baz (a)
	shardedIndex := baseIndex
	shardedIndex.KeyColumnNames = []string{"bucket_col", "a"}
	shardedIndex.KeyColumnIDs = descpb.ColumnIDs{0, 1}
	shardedIndex.Sharded = catpb.ShardedDescriptor{
		IsSharded:    true,
		ShardBuckets: 8,
		ColumnNames:  []string{"a"},
	}

	testData := []struct {
		index       descpb.IndexDescriptor
		tableName   tree.TableName
		partition   string
		displayMode IndexDisplayMode
		// if set to empty string, an error is expected to returned by
		// indexForDisplay
		expected   string
		pgExpected string
	}{
		{
			index:       baseIndex,
			tableName:   descpb.AnonymousTable,
			partition:   "",
			displayMode: IndexDisplayDefOnly,
			expected:    "INDEX baz (a ASC, b DESC)",
			pgExpected:  "INDEX baz USING btree (a ASC, b DESC)",
		},
		{
			index:       baseIndex,
			tableName:   descpb.AnonymousTable,
			partition:   "",
			displayMode: IndexDisplayShowCreate,
			expected:    "",
			pgExpected:  "",
		},
		{
			index:       baseIndex,
			tableName:   tableName,
			partition:   "",
			displayMode: IndexDisplayDefOnly,
			expected:    "INDEX baz ON foo.public.bar (a ASC, b DESC)",
			pgExpected:  "INDEX baz ON foo.public.bar USING btree (a ASC, b DESC)",
		},
		{
			index:       baseIndex,
			tableName:   tableName,
			partition:   "",
			displayMode: IndexDisplayShowCreate,
			expected:    "CREATE INDEX baz ON foo.public.bar (a ASC, b DESC)",
			pgExpected:  "CREATE INDEX baz ON foo.public.bar USING btree (a ASC, b DESC)",
		},
		{
			index:       uniqueIndex,
			tableName:   descpb.AnonymousTable,
			partition:   "",
			displayMode: IndexDisplayDefOnly,
			expected:    "UNIQUE INDEX baz (a ASC, b DESC)",
			pgExpected:  "UNIQUE INDEX baz USING btree (a ASC, b DESC)",
		},
		{
			index:       uniqueIndex,
			tableName:   tableName,
			partition:   "",
			displayMode: IndexDisplayShowCreate,
			expected:    "CREATE UNIQUE INDEX baz ON foo.public.bar (a ASC, b DESC)",
			pgExpected:  "CREATE UNIQUE INDEX baz ON foo.public.bar USING btree (a ASC, b DESC)",
		},
		{
			index:       jsonbInvertedIndex,
			tableName:   descpb.AnonymousTable,
			partition:   "",
			displayMode: IndexDisplayDefOnly,
			expected:    "INVERTED INDEX baz (a)",
			pgExpected:  "INDEX baz USING gin (a)",
		},
		{
			index:       jsonbInvertedIndex,
			tableName:   tableName,
			partition:   "",
			displayMode: IndexDisplayShowCreate,
			expected:    "CREATE INVERTED INDEX baz ON foo.public.bar (a)",
			pgExpected:  "CREATE INDEX baz ON foo.public.bar USING gin (a)",
		},
		{
			index:       storingIndex,
			tableName:   descpb.AnonymousTable,
			partition:   "",
			displayMode: IndexDisplayDefOnly,
			expected:    "INDEX baz (a ASC, b DESC) STORING (c)",
			pgExpected:  "INDEX baz USING btree (a ASC, b DESC) STORING (c)",
		},
		{
			index:       storingIndex,
			tableName:   tableName,
			partition:   "",
			displayMode: IndexDisplayShowCreate,
			expected:    "CREATE INDEX baz ON foo.public.bar (a ASC, b DESC) STORING (c)",
			pgExpected:  "CREATE INDEX baz ON foo.public.bar USING btree (a ASC, b DESC) STORING (c)",
		},
		{
			index:       partialIndex,
			tableName:   descpb.AnonymousTable,
			partition:   "",
			displayMode: IndexDisplayDefOnly,
			expected:    "INDEX baz (a ASC, b DESC) WHERE a > 1:::INT8",
			pgExpected:  "INDEX baz USING btree (a ASC, b DESC) WHERE (a > 1)",
		},
		{
			index:       partialIndex,
			tableName:   tableName,
			partition:   "",
			displayMode: IndexDisplayShowCreate,
			expected:    "CREATE INDEX baz ON foo.public.bar (a ASC, b DESC) WHERE a > 1:::INT8",
			pgExpected:  "CREATE INDEX baz ON foo.public.bar USING btree (a ASC, b DESC) WHERE (a > 1)",
		},
		{
			index:       expressionIndex,
			tableName:   descpb.AnonymousTable,
			partition:   "",
			displayMode: IndexDisplayDefOnly,
			expected:    "INDEX baz (a ASC, (a + b) DESC, b ASC)",
			pgExpected:  "INDEX baz USING btree (a ASC, (a + b) DESC, b ASC)",
		},
		{
			index:       expressionIndex,
			tableName:   tableName,
			partition:   "",
			displayMode: IndexDisplayShowCreate,
			expected:    "CREATE INDEX baz ON foo.public.bar (a ASC, (a + b) DESC, b ASC)",
			pgExpected:  "CREATE INDEX baz ON foo.public.bar USING btree (a ASC, (a + b) DESC, b ASC)",
		},
		{
			index:       partialIndex,
			tableName:   descpb.AnonymousTable,
			partition:   " PARTITION BY LIST (a) (PARTITION p VALUES IN (2))",
			displayMode: IndexDisplayDefOnly,
			expected:    "INDEX baz (a ASC, b DESC) PARTITION BY LIST (a) (PARTITION p VALUES IN (2)) WHERE a > 1:::INT8",
			pgExpected:  "INDEX baz USING btree (a ASC, b DESC) PARTITION BY LIST (a) (PARTITION p VALUES IN (2)) WHERE (a > 1)",
		},
		{
			index:       partialIndex,
			tableName:   tableName,
			partition:   " PARTITION BY LIST (a) (PARTITION p VALUES IN (2))",
			displayMode: IndexDisplayShowCreate,
			expected:    "CREATE INDEX baz ON foo.public.bar (a ASC, b DESC) PARTITION BY LIST (a) (PARTITION p VALUES IN (2)) WHERE a > 1:::INT8",
			pgExpected:  "CREATE INDEX baz ON foo.public.bar USING btree (a ASC, b DESC) PARTITION BY LIST (a) (PARTITION p VALUES IN (2)) WHERE (a > 1)",
		},
		{
			index:       shardedIndex,
			tableName:   descpb.AnonymousTable,
			partition:   "",
			displayMode: IndexDisplayDefOnly,
			expected:    "INDEX baz (a DESC) USING HASH WITH (bucket_count=8)",
			pgExpected:  "INDEX baz USING btree (a DESC) USING HASH WITH (bucket_count=8)",
		},
		{
			index:       shardedIndex,
			tableName:   tableName,
			partition:   "",
			displayMode: IndexDisplayShowCreate,
			expected:    "CREATE INDEX baz ON foo.public.bar (a DESC) USING HASH WITH (bucket_count=8)",
			pgExpected:  "CREATE INDEX baz ON foo.public.bar USING btree (a DESC) USING HASH WITH (bucket_count=8)",
		},
	}

	sd := &sessiondata.SessionData{}
	for testIdx, tc := range testData {
		t.Run(strconv.Itoa(testIdx), func(t *testing.T) {
			got, err := indexForDisplay(
				ctx,
				tableDesc,
				&tc.tableName,
				&tc.index,
				false, /* isPrimary */
				tc.partition,
				tree.FmtSimple,
				&semaCtx,
				sd,
				tc.displayMode,
			)

			if tc.expected == "" && err == nil {
				t.Fatalf("expected error but not failed")
			} else if tc.expected != "" && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if got != tc.expected {
				t.Errorf("expected '%s', got '%s'", tc.expected, got)
			}

			got, err = indexForDisplay(
				ctx,
				tableDesc,
				&tc.tableName,
				&tc.index,
				false, /* isPrimary */
				tc.partition,
				tree.FmtPGCatalog,
				&semaCtx,
				sd,
				tc.displayMode,
			)

			if tc.pgExpected == "" && err == nil {
				t.Fatalf("expected error but not failed")
			} else if tc.expected != "" && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}

			if got != tc.pgExpected {
				t.Errorf("expected '%s', got '%s'", tc.pgExpected, got)
			}
		})
	}
}
