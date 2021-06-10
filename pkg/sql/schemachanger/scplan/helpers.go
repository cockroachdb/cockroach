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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

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

func convertPrimaryIndexColumnDir(
	primaryIndex *scpb.PrimaryIndex,
) []descpb.IndexDescriptor_Direction {
	// Convert column directions
	convertedColumnDirs := make([]descpb.IndexDescriptor_Direction, 0, len(primaryIndex.KeyColumnDirections))
	for _, columnDir := range primaryIndex.KeyColumnDirections {
		switch columnDir {
		case scpb.PrimaryIndex_DESC:
			convertedColumnDirs = append(convertedColumnDirs, descpb.IndexDescriptor_DESC)
		case scpb.PrimaryIndex_ASC:
			convertedColumnDirs = append(convertedColumnDirs, descpb.IndexDescriptor_ASC)
		}
	}
	return convertedColumnDirs
}

func convertSecondaryIndexColumnDir(
	secondaryIndex *scpb.SecondaryIndex,
) []descpb.IndexDescriptor_Direction {
	// Convert column directions
	convertedColumnDirs := make([]descpb.IndexDescriptor_Direction, 0, len(secondaryIndex.KeyColumnDirections))
	for _, columnDir := range secondaryIndex.KeyColumnDirections {
		switch columnDir {
		case scpb.SecondaryIndex_DESC:
			convertedColumnDirs = append(convertedColumnDirs, descpb.IndexDescriptor_DESC)
		case scpb.SecondaryIndex_ASC:
			convertedColumnDirs = append(convertedColumnDirs, descpb.IndexDescriptor_ASC)
		}
	}
	return convertedColumnDirs
}
