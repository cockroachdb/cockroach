// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowenc

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
	s *fetchpb.IndexFetchSpec,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	index catalog.Index,
	fetchColumnIDs []descpb.ColumnID,
) error {
	oldFetchedCols := s.FetchedColumns
	*s = fetchpb.IndexFetchSpec{
		Version:             fetchpb.IndexFetchSpecVersionInitial,
		TableID:             table.GetID(),
		TableName:           table.GetName(),
		IndexID:             index.GetID(),
		IndexName:           index.GetName(),
		IsSecondaryIndex:    !index.Primary(),
		IsUniqueIndex:       index.IsUnique(),
		EncodingType:        index.GetEncodingType(),
		NumKeySuffixColumns: uint32(index.NumKeySuffixColumns()),
		GeoConfig:           index.GetGeoConfig(),
	}

	maxKeysPerRow := table.IndexKeysPerRow(index)
	s.MaxKeysPerRow = uint32(maxKeysPerRow)
	s.KeyPrefixLength = uint32(len(codec.TenantPrefix()) +
		encoding.EncodedLengthUvarintAscending(uint64(s.TableID)) +
		encoding.EncodedLengthUvarintAscending(uint64(index.GetID())))

	if ext := table.ExternalRowData(); ext != nil {
		newCodec := keys.MakeSQLCodec(ext.TenantID)
		newPrefix := newCodec.TablePrefix(uint32(ext.TableID))
		s.KeyPrefixLength = uint32(len(newPrefix) + encoding.EncodedLengthUvarintAscending(uint64(index.GetID())))
		s.External = &fetchpb.IndexFetchSpec_ExternalRowData{
			AsOf:     ext.AsOf,
			TenantID: ext.TenantID,
			TableID:  ext.TableID,
		}
	}

	s.FamilyDefaultColumns = table.FamilyDefaultColumns()

	families := table.GetFamilies()
	for i := range families {
		if id := families[i].ID; id > s.MaxFamilyID {
			s.MaxFamilyID = id
		}
	}

	s.KeyAndSuffixColumns = table.IndexFetchSpecKeyAndSuffixColumns(index)

	var invertedColumnID descpb.ColumnID
	if index.GetType() == idxtype.INVERTED {
		invertedColumnID = index.InvertedColumnID()
	}

	if cap(oldFetchedCols) >= len(fetchColumnIDs) {
		s.FetchedColumns = oldFetchedCols[:len(fetchColumnIDs)]
	} else {
		s.FetchedColumns = make([]fetchpb.IndexFetchSpec_Column, len(fetchColumnIDs))
	}
	for i, colID := range fetchColumnIDs {
		col, err := catalog.MustFindColumnByID(table, colID)
		if err != nil {
			return err
		}
		typ := col.GetType()
		if colID == invertedColumnID {
			typ = index.InvertedColumnKeyType()
		}
		s.FetchedColumns[i] = fetchpb.IndexFetchSpec_Column{
			Name:          col.GetName(),
			ColumnID:      colID,
			Type:          typ,
			IsNonNullable: !col.IsNullable() && col.Public(),
		}
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
