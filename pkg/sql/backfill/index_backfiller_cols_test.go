// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/stretchr/testify/require"
)

// Define some shorthand type names.
type (
	cols    = []fakeColumn
	index   = fakeIndex
	indexes = []index
	colIDs  = []descpb.ColumnID
)

// TestIndexBackfillerColumns exercises the logic in makeIndexBackfillerColumns
// to ensure that it properly classifies columns needed for evaluation in an
// index backfill.
func TestIndexBackfillerColumns(t *testing.T) {
	asIndexSlice := func(in indexes) (out []catalog.Index) {
		for _, idx := range in {
			out = append(out, idx)
		}
		return out
	}
	asColumnSlice := func(in cols) (out []catalog.Column) {
		for _, c := range in {
			out = append(out, c)
		}
		return out
	}
	for _, tc := range []struct {
		name        string
		cols        cols
		src         index
		toEncode    indexes
		expCols     colIDs
		expComputed colIDs
		expAdded    colIDs
		expErr      string
		expNeeded   colIDs
	}{
		{
			name: "boring two public columns in a secondary index",
			cols: cols{
				{id: 1, public: true},
				{id: 2, public: true},
			},
			src: index{
				primary:          true,
				keyCols:          colIDs{1},
				primaryValueCols: colIDs{2},
			},
			toEncode: indexes{
				{
					keyCols: colIDs{1},
				},
			},
			expCols:   colIDs{1, 2},
			expNeeded: colIDs{1},
		},
		{
			name: "one virtual, one computed adding mutation column in secondary",
			cols: cols{
				{id: 1, public: true},
				{id: 2, writeAndDeleteOnly: true, adding: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 2},
			},
			toEncode: indexes{
				{
					keyCols: colIDs{1, 3},
				},
				{
					keyCols: colIDs{3},
				},
			},
			expCols:     colIDs{1, 2, 3},
			expComputed: colIDs{3},
			expNeeded:   colIDs{1},
		},
		{
			name: "one virtual, one computed adding mutation column not used",
			cols: cols{
				{id: 1, public: true},
				{id: 2, writeAndDeleteOnly: true, adding: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 2},
			},
			toEncode: indexes{
				{
					keyCols: colIDs{1},
				},
				{
					keyCols: colIDs{1},
				},
			},
			expCols:     colIDs{1, 2, 3},
			expComputed: colIDs{3},
			expNeeded:   colIDs{1},
		},
		{
			name: "one virtual, one computed mutation column in primary",
			cols: cols{
				{id: 1, public: true},
				{id: 2, writeAndDeleteOnly: true, adding: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 2},
			},
			toEncode: indexes{
				{
					primary: true,
					keyCols: colIDs{1, 3},
				},
			},
			expCols:     colIDs{1, 2, 3},
			expComputed: colIDs{3},
			expNeeded:   colIDs{1, 2},
		},
		{
			// This is a weird case which wouldn't actually happen.
			name: "one virtual, one computed mutation column in source, not used in new primary",
			cols: cols{
				{id: 1, public: true},
				{id: 2, writeAndDeleteOnly: true, adding: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 2},
			},
			toEncode: indexes{
				{
					primary: true,
					keyCols: colIDs{1, 3},
				},
			},
			expCols:     colIDs{1, 2, 3},
			expComputed: colIDs{3},
			expNeeded:   colIDs{1, 2},
		},
		{
			// This is the case where we're building a new primary index as part
			// of an add column.
			name: "one virtual, one new mutation column in source used in new primary",
			cols: cols{
				{id: 1, public: true},
				{id: 2, writeAndDeleteOnly: true, adding: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 3},
			},
			toEncode: indexes{
				{
					primary: true,
					keyCols: colIDs{1, 2},
				},
			},
			expCols:     colIDs{1, 2, 3},
			expAdded:    colIDs{2},
			expComputed: colIDs{3},
			expNeeded:   colIDs{1},
		},
		{
			name: "dropped columns are excluded if not needed",
			cols: cols{
				{id: 1, public: true},
				{id: 2, writeAndDeleteOnly: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary:          true,
				keyCols:          colIDs{2},
				primaryValueCols: colIDs{1, 3},
			},
			toEncode: indexes{
				{
					primary: true,
					keyCols: colIDs{1, 3},
				},
			},
			expCols:     colIDs{1, 3},
			expComputed: colIDs{3},
			expNeeded:   colIDs{1},
		},
		{
			name: "dropped columns are included if needed",
			cols: cols{
				{id: 1, public: true},
				{id: 2, writeAndDeleteOnly: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary:          true,
				keyCols:          colIDs{2},
				primaryValueCols: colIDs{1, 3},
			},
			toEncode: indexes{
				{
					primary:          true,
					keyCols:          colIDs{1, 3},
					primaryValueCols: colIDs{2},
				},
			},
			expCols:     colIDs{1, 2, 3},
			expComputed: colIDs{3},
			expNeeded:   colIDs{1, 2},
		},
		{
			// This is the case where we're building a new primary index as part
			// of an add column for a computed stored column.
			name: "physical adding computed column in primary index",
			cols: cols{
				{id: 1, public: true},
				{id: 2, public: true, virtual: true, computed: true},
				{id: 3, adding: true, writeAndDeleteOnly: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 2},
			},
			toEncode: indexes{
				{
					primary: true,
					keyCols: colIDs{2, 1, 3},
				},
			},
			expCols:     colIDs{1, 2, 3},
			expNeeded:   colIDs{1},
			expComputed: colIDs{2, 3},
		},
		{
			name: "physical adding computed column in primary index but not adding",
			cols: cols{
				{id: 1, public: true},
				{id: 2, public: true, virtual: true, computed: true},
				{id: 3, writeAndDeleteOnly: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 2},
			},
			toEncode: indexes{
				{
					primary: true,
					keyCols: colIDs{2, 1, 3},
				},
			},
			expErr: "index being backfilled contains non-writable or dropping column",
		},
		{
			name: "physical adding computed column in primary index but not writable",
			cols: cols{
				{id: 1, public: true},
				{id: 2, public: true, virtual: true, computed: true},
				{id: 3, adding: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 2},
			},
			toEncode: indexes{
				{
					primary: true,
					keyCols: colIDs{2, 1, 3},
				},
			},
			expErr: "index being backfilled contains non-writable or dropping column",
		},
		{
			name: "secondary index with new column",
			cols: cols{
				{id: 1, public: true},
				{id: 2, adding: true, writeAndDeleteOnly: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 3},
			},
			toEncode: indexes{
				{
					keyCols: colIDs{1, 2, 3},
				},
			},
			expErr: "secondary index for backfill contains physical column not present in source primary index",
		},
		{
			name: "secondary index and primary index with new column",
			cols: cols{
				{id: 1, public: true},
				{id: 2, adding: true, writeAndDeleteOnly: true},
				{id: 3, public: true, virtual: true, computed: true},
			},
			src: index{
				primary: true,
				keyCols: colIDs{1, 3},
			},
			toEncode: indexes{
				{
					primary: true,
					keyCols: colIDs{1, 2},
				},
				{
					keyCols: colIDs{1, 2, 3},
				},
			},
			expErr: "secondary index for backfill contains physical column not present in source primary index",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := makeIndexBackfillColumns(
				asColumnSlice(tc.cols), tc.src, asIndexSlice(tc.toEncode),
			)
			if tc.expErr != "" {
				require.Regexp(t, tc.expErr, err)
				return
			}

			// Validate that all the columns are classified as expected.
			idToCol := make(map[descpb.ColumnID]catalog.Column)
			for _, c := range tc.cols {
				idToCol[c.id] = c
			}
			toColumnSlice := func(ids colIDs) (out []catalog.Column) {
				for _, id := range ids {
					out = append(out, idToCol[id])
				}
				return out
			}

			require.Equal(t, toColumnSlice(tc.expCols), out.cols)
			require.Equal(t, toColumnSlice(tc.expComputed), out.computedCols)
			require.Equal(t, toColumnSlice(tc.expAdded), out.addedCols)
			var needed catalog.TableColSet
			out.valNeededForCol.ForEach(func(i int) {
				needed.Add(out.cols[i].GetID())
			})
			require.Equal(t, catalog.MakeTableColSet(tc.expNeeded...).Ordered(), needed.Ordered())
		})

	}
}

// TestInitIndexesAllowList tests that initIndexes works correctly with
// "allowList" to populate the "added" field of the index backfiller.
func TestInitIndexesAllowList(t *testing.T) {
	desc := &tabledesc.Mutable{}
	desc.TableDescriptor = descpb.TableDescriptor{
		Mutations: []descpb.DescriptorMutation{
			{
				// candidate 1
				Descriptor_: &descpb.DescriptorMutation_Index{
					Index: &descpb.IndexDescriptor{ID: 2},
				},
				Direction: descpb.DescriptorMutation_ADD,
			},
			{
				// candidate 2
				Descriptor_: &descpb.DescriptorMutation_Index{
					Index: &descpb.IndexDescriptor{ID: 3},
				},
				Direction: descpb.DescriptorMutation_ADD,
			},
			{
				// non-candidate: index is being dropped
				Descriptor_: &descpb.DescriptorMutation_Index{
					Index: &descpb.IndexDescriptor{ID: 4},
				},
				Direction: descpb.DescriptorMutation_DROP,
			},
			{
				// non-candidate: index is temporary index
				Descriptor_: &descpb.DescriptorMutation_Index{
					Index: &descpb.IndexDescriptor{ID: 4, UseDeletePreservingEncoding: true},
				},
				Direction: descpb.DescriptorMutation_ADD,
			},
			{
				// non-candidate: not an index
				Descriptor_: &descpb.DescriptorMutation_Column{
					Column: &descpb.ColumnDescriptor{ID: 2},
				},
				Direction: descpb.DescriptorMutation_ADD,
			},
		},
	}

	t.Run("nil allowList", func(t *testing.T) {
		// A nil allowList means no filtering.
		ib := &IndexBackfiller{}
		ib.initIndexes(keys.SystemSQLCodec, desc, nil /* allowList */)
		require.Equal(t, 2, len(ib.added))
		require.Equal(t, catid.IndexID(2), ib.added[0].GetID())
		require.Equal(t, catid.IndexID(3), ib.added[1].GetID())
	})

	t.Run("non-nil allowList", func(t *testing.T) {
		ib := &IndexBackfiller{}
		ib.initIndexes(keys.SystemSQLCodec, desc, []catid.IndexID{3} /* allowList */)
		require.Equal(t, 1, len(ib.added))
		require.Equal(t, catid.IndexID(3), ib.added[0].GetID())
	})
}

type fakeColumn struct {
	catalog.Column
	id                                 descpb.ColumnID
	public, adding, writeAndDeleteOnly bool
	computed, virtual                  bool
}

func (fc fakeColumn) Public() bool             { return fc.public }
func (fc fakeColumn) Adding() bool             { return fc.adding }
func (fc fakeColumn) Dropped() bool            { return fc.writeAndDeleteOnly }
func (fc fakeColumn) WriteAndDeleteOnly() bool { return fc.writeAndDeleteOnly }
func (fc fakeColumn) IsComputed() bool         { return fc.computed }
func (fc fakeColumn) IsVirtual() bool          { return fc.virtual }

func (fc fakeColumn) GetID() descpb.ColumnID {
	return fc.id
}

type fakeIndex struct {
	catalog.Index
	primary            bool
	keyCols            []descpb.ColumnID
	keySuffixCols      []descpb.ColumnID
	secondaryValueCols []descpb.ColumnID
	primaryValueCols   []descpb.ColumnID
}

func (fi fakeIndex) CollectKeyColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(fi.keyCols...)
}
func (fi fakeIndex) CollectKeySuffixColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(fi.keySuffixCols...)
}
func (fi fakeIndex) CollectPrimaryStoredColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(fi.primaryValueCols...)
}
func (fi fakeIndex) CollectSecondaryStoredColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(fi.secondaryValueCols...)
}

func (fi fakeIndex) GetEncodingType() catenumpb.IndexDescriptorEncodingType {
	if fi.primary {
		return catenumpb.PrimaryIndexEncoding
	}
	return catenumpb.SecondaryIndexEncoding
}
