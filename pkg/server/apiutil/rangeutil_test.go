// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiutil_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/apiutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/stretchr/testify/require"
)

func TestMapRangesToIndexes(t *testing.T) {
	k := func(c byte) roachpb.Key {
		return roachpb.Key([]byte{c})
	}
	rk := func(c byte) roachpb.RKey {
		return roachpb.RKey(k(c))
	}
	ranges := []roachpb.RangeDescriptor{
		{RangeID: 1, StartKey: rk('d'), EndKey: rk('m')},
		{RangeID: 2, StartKey: rk('m'), EndKey: rk('x')},
	}

	indexes := []apiutil.IndexNames{
		{Index: "Totally before first", Span: roachpb.Span{Key: k('a'), EndKey: k('c')}},
		{Index: "Start and before first", Span: roachpb.Span{Key: k('c'), EndKey: k('e')}},
		{Index: "Middle of first range", Span: roachpb.Span{Key: k('e'), EndKey: k('f')}},
		{Index: "Overlaps with both", Span: roachpb.Span{Key: k('f'), EndKey: k('o')}},
		{Index: "Middle of second range", Span: roachpb.Span{Key: k('o'), EndKey: k('q')}},
		{Index: "End and after second", Span: roachpb.Span{Key: k('q'), EndKey: k('y')}},
		{Index: "Totally after end", Span: roachpb.Span{Key: k('y'), EndKey: k('z')}},
	}

	expected := map[roachpb.RangeID][]apiutil.IndexNames{
		1: {
			{Index: "Start and before first"},
			{Index: "Middle of first range"},
			{Index: "Overlaps with both"},
		},
		2: {
			{Index: "Overlaps with both"},
			{Index: "Middle of second range"},
			{Index: "End and after second"},
		},
	}

	result := apiutil.MapRangesToIndexes(ranges, indexes)

	require.Equal(t, len(result), len(expected))
	b, _ := json.MarshalIndent(result, "", "\t")
	fmt.Println(string(b))
	b, _ = json.MarshalIndent(indexes, "", "\t")
	fmt.Println(string(b))

	for rangeID, expectedIndexes := range expected {
		actualIndexes, ok := result[rangeID]
		require.True(t, ok)
		require.Equal(t, len(actualIndexes), len(expectedIndexes))
		for i, expectedIndex := range expectedIndexes {
			fmt.Println(rangeID, i, expectedIndex, actualIndexes[i])
			require.Equal(t, expectedIndex.Index, actualIndexes[i].Index)
		}
	}
}

func TestRangesToTableSpans(t *testing.T) {
	codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(1))
	ranges := []roachpb.RangeDescriptor{
		// should be zero len
		{
			StartKey: roachpb.RKey(codec.TablePrefix(0).Prevish(1)),
			EndKey:   roachpb.RKey(codec.TablePrefix(0)),
		},
		// should also be zero len
		{
			StartKey: roachpb.RKey(codec.TablePrefix(1)),
			EndKey:   roachpb.RKey(codec.TablePrefix(1)),
		},
		{
			StartKey: roachpb.RKey(codec.TablePrefix(1)),
			EndKey:   roachpb.RKey(codec.TablePrefix(3)),
		},
		{
			StartKey: roachpb.RKey(codec.TablePrefix(3)),
			EndKey:   roachpb.RKey(codec.TablePrefix(5)),
		},
		{
			StartKey: roachpb.RKey(codec.TablePrefix(5)),
			EndKey:   roachpb.RKey(codec.TablePrefix(6)),
		},
	}

	expected := []roachpb.Span{
		{
			Key:    codec.TablePrefix(1),
			EndKey: codec.TablePrefix(3),
		},
		{
			Key:    codec.TablePrefix(3),
			EndKey: codec.TablePrefix(5),
		},
		{
			Key:    codec.TablePrefix(5),
			EndKey: codec.TablePrefix(6),
		},
	}

	result, err := apiutil.RangesToTableSpans(codec, ranges)

	require.NoError(t, err)
	require.Equal(t, len(result), len(expected))

	for i, span := range expected {
		if !result[i].Equal(span) {
			t.Fatalf("expected span %v, got %v", span, result[i])
		}
	}
}

func makeDBDesc(id uint32, name string) catalog.DatabaseDescriptor {

	db := &dbdesc.Mutable{}
	db.SetName(name)
	db.ID = catid.DescID(id)
	descriptor := db.ImmutableCopy()
	return descriptor.(catalog.DatabaseDescriptor)
}

func makeTableDesc(databaseID uint32, name string, indexes []string) catalog.TableDescriptor {
	descIndexes := []descpb.IndexDescriptor{}
	table := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name:     name,
		ParentID: catid.DescID(databaseID),
		Indexes:  descIndexes,
	}).BuildCreatedMutableTable()
	for i, name := range indexes {
		if i == 0 {
			_ = table.AddPrimaryIndex(descpb.IndexDescriptor{Name: name})
		} else {

			_ = table.AddSecondaryIndex(descpb.IndexDescriptor{Name: name})
		}
	}
	return table.NewBuilder().BuildImmutable().(catalog.TableDescriptor)
}

func TestTableDescriptorsToIndexNames(t *testing.T) {
	// test straightforward path with three tables, two databases
	codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(1))
	databases := map[descpb.ID]catalog.DatabaseDescriptor{
		1: makeDBDesc(1, "db1"),
		2: makeDBDesc(2, "db2"),
	}
	tables := []catalog.TableDescriptor{
		makeTableDesc(1, "table1", []string{"pkey"}),
		makeTableDesc(2, "table2", []string{"pkey", "table2_secondary_column"}),
		makeTableDesc(2, "table3", []string{"pkey"}),
	}

	expected := []apiutil.IndexNames{
		{Database: "db1", Table: "table1", Index: "pkey"},
		{Database: "db2", Table: "table2", Index: "pkey"},
		{Database: "db2", Table: "table2", Index: "table2_secondary_column"},
		{Database: "db2", Table: "table3", Index: "pkey"},
	}
	indexes, err := apiutil.TableDescriptorsToIndexNames(codec, databases, tables)

	require.NoError(t, err)
	require.Equal(t, len(expected), len(indexes))
	for i, index := range indexes {
		if !index.Equal(expected[i]) {
			t.Fatalf("resulting index did not match expected output: %s %s", index, expected[i])
		}
	}
}

func TestTableDescriptorsToIndexNamesDeduplicates(t *testing.T) {
	// verify that duplicate descriptors are de-duplicated
	codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(1))
	databases := map[descpb.ID]catalog.DatabaseDescriptor{
		1: makeDBDesc(1, "db1"),
	}
	tables := []catalog.TableDescriptor{
		makeTableDesc(1, "table1", []string{"pkey", "table1_secondary_column"}),
		makeTableDesc(1, "table1", []string{"pkey", "table1_secondary_column"}),
	}

	expected := []apiutil.IndexNames{
		{Database: "db1", Table: "table1", Index: "pkey"},
		{Database: "db1", Table: "table1", Index: "table1_secondary_column"},
	}
	indexes, err := apiutil.TableDescriptorsToIndexNames(codec, databases, tables)

	require.NoError(t, err)
	require.Equal(t, len(expected), len(indexes))
	for i, index := range indexes {
		if !index.Equal(expected[i]) {
			t.Fatalf("resulting index did not match expected output: %s %s", index, expected[i])
		}
	}
}

func TestGetIndexNamesFromDescriptorsMissingDatabase(t *testing.T) {
	codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(1))
	databases := map[descpb.ID]catalog.DatabaseDescriptor{
		1: makeDBDesc(1, "db1"),
	}
	tables := []catalog.TableDescriptor{
		makeTableDesc(2, "table2", []string{"pkey", "table2_secondary_column"}),
	}

	_, err := apiutil.TableDescriptorsToIndexNames(codec, databases, tables)
	require.Errorf(t, err, "could not find database for table %s", tables[0].GetName())
}
