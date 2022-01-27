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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
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
// The expectation here, therefore, is that this function is called either for a
// new descriptor that doesn't exist in storage yet, or already has its
// modification time field (and others which depend on the MVCC timestamp)
// set to a valid value.
func NewBuilder(desc *descpb.Descriptor) catalog.DescriptorBuilder {
	return NewBuilderWithMVCCTimestamp(desc, hlc.Timestamp{})
}

// ValidateSelf validates that the descriptor is internally consistent.
func ValidateSelf(desc catalog.Descriptor, version clusterversion.ClusterVersion) error {
	return validate.Self(version, desc)
}
