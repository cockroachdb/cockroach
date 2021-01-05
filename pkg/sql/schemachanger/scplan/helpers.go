package scplan

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

func indexContainsColumn(idx *descpb.IndexDescriptor, colID descpb.ColumnID) bool {
	return columnsContainsID(idx.ColumnIDs, colID) ||
		columnsContainsID(idx.StoreColumnIDs, colID) ||
		columnsContainsID(idx.ExtraColumnIDs, colID)
}

func columnsContainsID(haystack []descpb.ColumnID, needle descpb.ColumnID) bool {
	for _, id := range haystack {
		if id == needle {
			return true
		}
	}
	return false
}
