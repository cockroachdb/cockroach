// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// secIdx describes a secondary index for test construction. The idxType field
// specifies the index type (forward, inverted, or vector).
type secIdx struct {
	colIDs  []descpb.ColumnID
	dirs    []catenumpb.IndexColumn_Direction
	idxType idxtype.T
}

// makeTableWithIndexes builds an immutable table descriptor with the given
// primary index and secondary indexes. Each index is described by its key
// column IDs, directions, and index type.
func makeTableWithIndexes(
	t *testing.T,
	pkColIDs []descpb.ColumnID,
	pkDirs []catenumpb.IndexColumn_Direction,
	secondaryIndexes []secIdx,
) catalog.TableDescriptor {
	t.Helper()

	// Build column names for each column ID referenced. We use simple names.
	colNames := func(ids []descpb.ColumnID) []string {
		names := make([]string, len(ids))
		for i, id := range ids {
			names[i] = string(rune('a'-1+int(id))) + "_col"
		}
		return names
	}

	// Build columns slice covering all referenced IDs.
	maxID := descpb.ColumnID(0)
	for _, id := range pkColIDs {
		if id > maxID {
			maxID = id
		}
	}
	for _, si := range secondaryIndexes {
		for _, id := range si.colIDs {
			if id > maxID {
				maxID = id
			}
		}
	}
	columns := make([]descpb.ColumnDescriptor, maxID)
	for i := descpb.ColumnID(1); i <= maxID; i++ {
		columns[i-1] = descpb.ColumnDescriptor{
			ID:   i,
			Name: string(rune('a'-1+int(i))) + "_col",
		}
	}

	primaryIndex := descpb.IndexDescriptor{
		ID:                  1,
		Name:                "pk",
		Unique:              true,
		KeyColumnIDs:        pkColIDs,
		KeyColumnNames:      colNames(pkColIDs),
		KeyColumnDirections: pkDirs,
		ConstraintID:        1,
		EncodingType:        catenumpb.PrimaryIndexEncoding,
	}

	indexes := make([]descpb.IndexDescriptor, len(secondaryIndexes))
	for i, si := range secondaryIndexes {
		idx := descpb.IndexDescriptor{
			ID:                  descpb.IndexID(i + 2),
			Name:                string(rune('a'+i)) + "_idx",
			KeyColumnIDs:        si.colIDs,
			KeyColumnNames:      colNames(si.colIDs),
			KeyColumnDirections: si.dirs,
			ConstraintID:        descpb.ConstraintID(i + 2),
		}
		idx.Type = si.idxType
		if si.idxType == idxtype.INVERTED {
			idx.InvertedColumnKinds = []catpb.InvertedIndexColumnKind{
				catpb.InvertedIndexColumnKind_DEFAULT,
			}
		}
		indexes[i] = idx
	}

	td := descpb.TableDescriptor{
		ID:            100,
		Name:          "test_table",
		ParentID:      1,
		Columns:       columns,
		PrimaryIndex:  primaryIndex,
		Indexes:       indexes,
		NextColumnID:  maxID + 1,
		NextIndexID:   descpb.IndexID(len(secondaryIndexes) + 2),
		NextFamilyID:  1,
		FormatVersion: descpb.InterleavedFormatVersion,
		Families: []descpb.ColumnFamilyDescriptor{{
			ID:   0,
			Name: "primary",
		}},
	}
	// Add all columns to family 0.
	for _, col := range columns {
		td.Families[0].ColumnIDs = append(td.Families[0].ColumnIDs, col.ID)
		td.Families[0].ColumnNames = append(td.Families[0].ColumnNames, col.Name)
	}

	b := tabledesc.NewBuilder(&td)
	require.NoError(t, b.RunPostDeserializationChanges())
	return b.BuildImmutableTable()
}

func dirs(d ...catenumpb.IndexColumn_Direction) []catenumpb.IndexColumn_Direction {
	return d
}

func ids(id ...descpb.ColumnID) []descpb.ColumnID {
	return id
}

var asc = catenumpb.IndexColumn_ASC
var desc = catenumpb.IndexColumn_DESC

func TestIsBackfillDataSorted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name     string
		pkIDs    []descpb.ColumnID
		pkDirs   []catenumpb.IndexColumn_Direction
		destIdx  secIdx
		expected bool
	}{{
		name:     "pk=(a,b) idx=(a,b) sorted",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(1, 2), dirs: dirs(asc, asc)},
		expected: true,
	}, {
		name:     "pk=(a,b) idx=(a,b,c) sorted",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(1, 2, 3), dirs: dirs(asc, asc, asc)},
		expected: true,
	}, {
		name:     "pk=(a,b) idx=(a,c) not sorted",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(1, 3), dirs: dirs(asc, asc)},
		expected: false,
	}, {
		name:     "pk=(a,b) idx=(a) sorted",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(1), dirs: dirs(asc)},
		expected: true,
	}, {
		name:     "pk=(a,b,c) idx=(a,b) sorted",
		pkIDs:    ids(1, 2, 3),
		pkDirs:   dirs(asc, asc, asc),
		destIdx:  secIdx{colIDs: ids(1, 2), dirs: dirs(asc, asc)},
		expected: true,
	}, {
		name:     "pk=(a,b) idx=(a DESC, b DESC, c) sorted (direction ignored)",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(1, 2, 3), dirs: dirs(desc, desc, asc)},
		expected: true,
	}, {
		name:     "pk=(a,b) idx=(c,d) not sorted",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(3, 4), dirs: dirs(asc, asc)},
		expected: false,
	}, {
		name:     "pk=(a,b) idx=(b,a) not sorted",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(2, 1), dirs: dirs(asc, asc)},
		expected: false,
	}, {
		name:     "pk=(a,b) idx=(a INVERTED) not sorted",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(1), dirs: dirs(asc), idxType: idxtype.INVERTED},
		expected: false,
	}, {
		name:     "pk=(a,b) idx=(a VECTOR) not sorted",
		pkIDs:    ids(1, 2),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(1), dirs: dirs(asc), idxType: idxtype.VECTOR},
		expected: false,
	}, {
		name:     "pk=(shard,a) idx=(a,b) not sorted",
		pkIDs:    ids(4, 1),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(1, 2), dirs: dirs(asc, asc)},
		expected: false,
	}, {
		name:     "pk=(shard,a) idx=(shard,a,b) sorted",
		pkIDs:    ids(4, 1),
		pkDirs:   dirs(asc, asc),
		destIdx:  secIdx{colIDs: ids(4, 1, 2), dirs: dirs(asc, asc, asc)},
		expected: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			td := makeTableWithIndexes(t, tt.pkIDs, tt.pkDirs, []secIdx{tt.destIdx})
			sourceIdx := td.GetPrimaryIndex()
			destIdx := td.PublicNonPrimaryIndexes()[0]
			result := backfill.IsBackfillDataSorted(sourceIdx, destIdx)
			require.Equal(t, tt.expected, result)
		})
	}
}
