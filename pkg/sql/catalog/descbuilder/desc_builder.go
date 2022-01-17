// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catval"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// NewBuilderWithMVCCTimestamp takes a descriptor as deserialized from storage,
// along with its MVCC timestamp, and returns a catalog.DescriptorBuilder object.
// Returns nil if nothing specific is found in desc.
func NewBuilderWithMVCCTimestamp(
	desc *descpb.Descriptor, mvccTimestamp hlc.Timestamp,
) catalog.DescriptorBuilder {
	table, database, typ, schema := descpb.FromDescriptorWithMVCCTimestamp(desc, mvccTimestamp)
	switch {
	case table != nil:
		return tabledesc.NewBuilder(table)
	case database != nil:
		return dbdesc.NewBuilder(database)
	case typ != nil:
		return typedesc.NewBuilder(typ)
	case schema != nil:
		return schemadesc.NewBuilder(schema)
	default:
		return nil
	}
}

// NewBuilder is a convenience function which calls NewBuilderWithMVCCTimestamp
// with an empty timestamp.
func NewBuilder(desc *descpb.Descriptor) catalog.DescriptorBuilder {
	return NewBuilderWithMVCCTimestamp(desc, hlc.Timestamp{})
}

// ValidateSelf validates that the descriptor is internally consistent.
func ValidateSelf(desc catalog.Descriptor) error {
	return catval.ValidateSelf(desc)
}
