// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

func indexContainsColumn(idx *descpb.IndexDescriptor, colID descpb.ColumnID) bool {
	return columnsContainsID(idx.KeyColumnIDs, colID) ||
		columnsContainsID(idx.StoreColumnIDs, colID) ||
		columnsContainsID(idx.KeySuffixColumnIDs, colID)
}

func columnsContainsID(haystack []descpb.ColumnID, needle descpb.ColumnID) bool {
	for _, id := range haystack {
		if id == needle {
			return true
		}
	}
	return false
}
