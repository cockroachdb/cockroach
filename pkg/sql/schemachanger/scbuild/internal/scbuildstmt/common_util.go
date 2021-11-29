// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

func onErrPanic(err error) {
	if err != nil {
		panic(err)
	}
}

// primaryIndexElemFromDescriptor constructs a primary index element from an
// index descriptor.
func primaryIndexElemFromDescriptor(
	indexDesc *descpb.IndexDescriptor, tbl catalog.TableDescriptor,
) (*scpb.PrimaryIndex, *scpb.IndexName) {
	if len(indexDesc.Partitioning.Range) > 0 ||
		len(indexDesc.Partitioning.List) > 0 {
		panic(scerrors.NotImplementedErrorf(nil, "partitioning on new indexes is not supported."))
	}
	keyColumnDirs := make([]scpb.PrimaryIndex_Direction, 0, len(indexDesc.KeyColumnDirections))
	for _, dir := range indexDesc.KeyColumnDirections {
		switch dir {
		case descpb.IndexDescriptor_DESC:
			keyColumnDirs = append(keyColumnDirs, scpb.PrimaryIndex_DESC)
		case descpb.IndexDescriptor_ASC:
			keyColumnDirs = append(keyColumnDirs, scpb.PrimaryIndex_ASC)
		default:
			panic(errors.AssertionFailedf("Unknown direction type %s", dir))
		}
	}
	return &scpb.PrimaryIndex{TableID: tbl.GetID(),
			IndexID:             indexDesc.ID,
			Unique:              indexDesc.Unique,
			KeyColumnIDs:        indexDesc.KeyColumnIDs,
			KeyColumnDirections: keyColumnDirs,
			KeySuffixColumnIDs:  indexDesc.KeySuffixColumnIDs,
			StoringColumnIDs:    indexDesc.StoreColumnIDs,
			CompositeColumnIDs:  indexDesc.CompositeColumnIDs,
			Inverted:            indexDesc.Type == descpb.IndexDescriptor_INVERTED,
			ShardedDescriptor:   &indexDesc.Sharded},
		&scpb.IndexName{
			TableID: tbl.GetID(),
			IndexID: indexDesc.ID,
			Name:    indexDesc.Name,
		}
}

// secondaryIndexElemFromDescriptor constructs a secondary index element from an
// index descriptor.
func secondaryIndexElemFromDescriptor(
	indexDesc *descpb.IndexDescriptor, tbl catalog.TableDescriptor,
) (*scpb.SecondaryIndex, *scpb.IndexName) {
	if len(indexDesc.Partitioning.Range) > 0 ||
		len(indexDesc.Partitioning.List) > 0 {
		panic(scerrors.NotImplementedErrorf(nil, "partitioning on new indexes is not supported."))
	}
	keyColumnDirs := make([]scpb.SecondaryIndex_Direction, 0, len(indexDesc.KeyColumnDirections))
	for _, dir := range indexDesc.KeyColumnDirections {
		switch dir {
		case descpb.IndexDescriptor_DESC:
			keyColumnDirs = append(keyColumnDirs, scpb.SecondaryIndex_DESC)
		case descpb.IndexDescriptor_ASC:
			keyColumnDirs = append(keyColumnDirs, scpb.SecondaryIndex_ASC)
		default:
			panic(errors.AssertionFailedf("Unknown direction type %s", dir))
		}
	}
	return &scpb.SecondaryIndex{TableID: tbl.GetID(),
			IndexID:             indexDesc.ID,
			Unique:              indexDesc.Unique,
			KeyColumnIDs:        indexDesc.KeyColumnIDs,
			KeyColumnDirections: keyColumnDirs,
			KeySuffixColumnIDs:  indexDesc.KeySuffixColumnIDs,
			StoringColumnIDs:    indexDesc.StoreColumnIDs,
			CompositeColumnIDs:  indexDesc.CompositeColumnIDs,
			Inverted:            indexDesc.Type == descpb.IndexDescriptor_INVERTED,
			ShardedDescriptor:   &indexDesc.Sharded},
		&scpb.IndexName{
			TableID: tbl.GetID(),
			IndexID: indexDesc.ID,
			Name:    indexDesc.Name,
		}
}
