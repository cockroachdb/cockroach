// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// InitIndexFetchSpec fills in an IndexFetchSpec for the given index and
// provided fetch columns. All the fields are reinitialized; the slices are
// reused if they have enough capacity.
//
// The fetch columns are assumed to be available in the index. If the index is
// inverted and we fetch the inverted key, the corresponding Column contains the
// inverted column type.
func InitIndexFetchSpec(
	s *descpb.IndexFetchSpec,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	index catalog.Index,
	fetchColumnIDs []descpb.ColumnID,
) error {
	oldKeyAndSuffixCols := s.KeyAndSuffixColumns
	oldFetchedCols := s.FetchedColumns
	oldFamilies := s.FamilyDefaultColumns
	*s = descpb.IndexFetchSpec{
		Version:             descpb.IndexFetchSpecVersionInitial,
		TableName:           table.GetName(),
		TableID:             table.GetID(),
		IndexName:           index.GetName(),
		IsSecondaryIndex:    !index.Primary(),
		IsUniqueIndex:       index.IsUnique(),
		EncodingType:        index.GetEncodingType(),
		NumKeySuffixColumns: uint32(index.NumKeySuffixColumns()),
	}

	indexID := index.GetID()
	maxKeysPerRow, err := table.KeysPerRow(indexID)
	if err != nil {
		return err
	}
	s.MaxKeysPerRow = uint32(maxKeysPerRow)
	s.KeyPrefixLength = uint32(len(MakeIndexKeyPrefix(codec, table.GetID(), indexID)))

	families := table.GetFamilies()
	for i := range families {
		f := &families[i]
		if f.DefaultColumnID != 0 {
			if s.FamilyDefaultColumns == nil {
				s.FamilyDefaultColumns = oldFamilies[:0]
			}
			s.FamilyDefaultColumns = append(s.FamilyDefaultColumns, descpb.IndexFetchSpec_FamilyDefaultColumn{
				FamilyID:        f.ID,
				DefaultColumnID: f.DefaultColumnID,
			})
		}
		if f.ID > s.MaxFamilyID {
			s.MaxFamilyID = f.ID
		}
	}

	indexCols := table.IndexColumns(index)
	keyDirs := table.IndexFullColumnDirections(index)
	compositeIDs := index.CollectCompositeColumnIDs()

	var invertedColumnID descpb.ColumnID
	if index.GetType() == descpb.IndexDescriptor_INVERTED {
		invertedColumnID = index.InvertedColumnID()
	}

	mkCol := func(col catalog.Column, colID descpb.ColumnID) descpb.IndexFetchSpec_Column {
		typ := col.GetType()
		if colID == invertedColumnID {
			typ = index.InvertedColumnKeyType()
		}
		return descpb.IndexFetchSpec_Column{
			Name:          col.GetName(),
			ColumnID:      colID,
			Type:          typ,
			IsNonNullable: !col.IsNullable() && col.Public(),
		}
	}

	numKeyCols := index.NumKeyColumns() + index.NumKeySuffixColumns()
	if cap(oldKeyAndSuffixCols) >= numKeyCols {
		s.KeyAndSuffixColumns = oldKeyAndSuffixCols[:numKeyCols]
	} else {
		s.KeyAndSuffixColumns = make([]descpb.IndexFetchSpec_KeyColumn, numKeyCols)
	}
	for i := range s.KeyAndSuffixColumns {
		col := indexCols[i]
		if !col.Public() {
			// Key columns must be public.
			return fmt.Errorf("column %q (%d) is not public", col.GetName(), col.GetID())
		}
		colID := col.GetID()
		dir := descpb.IndexDescriptor_ASC
		// If this is a unique index, the suffix columns are not part of the full
		// index columns and are always ascending.
		if i < len(keyDirs) {
			dir = keyDirs[i]
		}
		s.KeyAndSuffixColumns[i] = descpb.IndexFetchSpec_KeyColumn{
			IndexFetchSpec_Column: mkCol(col, colID),
			Direction:             dir,
			IsComposite:           compositeIDs.Contains(colID),
			IsInverted:            colID == invertedColumnID,
		}
	}

	if cap(oldFetchedCols) >= len(fetchColumnIDs) {
		s.FetchedColumns = oldFetchedCols[:len(fetchColumnIDs)]
	} else {
		s.FetchedColumns = make([]descpb.IndexFetchSpec_Column, len(fetchColumnIDs))
	}
	for i, colID := range fetchColumnIDs {
		col, err := table.FindColumnWithID(colID)
		if err != nil {
			return err
		}
		s.FetchedColumns[i] = mkCol(col, colID)
	}

	// In test builds, verify that we aren't trying to fetch columns that are not
	// available in the index.
	if buildutil.CrdbTestBuild && s.IsSecondaryIndex {
		colIDs := index.CollectKeyColumnIDs()
		colIDs.UnionWith(index.CollectSecondaryStoredColumnIDs())
		colIDs.UnionWith(index.CollectKeySuffixColumnIDs())
		for i := range s.FetchedColumns {
			if !colIDs.Contains(s.FetchedColumns[i].ColumnID) {
				return errors.AssertionFailedf("requested column %s not in index", s.FetchedColumns[i].Name)
			}
		}
	}

	return nil
}
