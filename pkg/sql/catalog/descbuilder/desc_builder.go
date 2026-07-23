// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// FromSerializedValue deserializes a descriptor from a *roachpb.Value and
// properly sets its timestamps based on the MVCC timestamp before returning it
// as a builder if it's not empty.
//
// It is vital that users which read table descriptor values from the KV store
// call this method.
//
// When table descriptor versions are incremented they are written with a
// zero-valued ModificationTime. This is done to avoid the need to observe
// the commit timestamp for the writing transaction which would prevent
// pushes. This method is used in the read path to set the modification time
// based on the MVCC timestamp of row which contained this descriptor. If
// the ModificationTime is non-zero then we know that either this table
// descriptor was written by older version of cockroach which included the
// exact commit timestamp or it was re-written in which case it will include
// a timestamp which was set by this method.
func FromSerializedValue(value *roachpb.Value) (catalog.DescriptorBuilder, error) {
	if value == nil {
		return nil, nil
	}
	var descProto descpb.Descriptor
	if err := value.GetProto(&descProto); err != nil {
		return nil, err
	}
	return fromProtobufAndMVCCTimestamp(&descProto, value.Timestamp)
}

// FromBytesAndMVCCTimestamp is very similar to FromSerializedValue but does
// not feature a tag in the bytes slice.
func FromBytesAndMVCCTimestamp(
	descBytes []byte, mvccTimestamp hlc.Timestamp,
) (catalog.DescriptorBuilder, error) {
	var descProto descpb.Descriptor
	if err := protoutil.Unmarshal(descBytes, &descProto); err != nil {
		return nil, err
	}
	return fromProtobufAndMVCCTimestamp(&descProto, mvccTimestamp)
}

func fromProtobufAndMVCCTimestamp(
	descProto *descpb.Descriptor, mvccTimestamp hlc.Timestamp,
) (catalog.DescriptorBuilder, error) {
	b := newBuilder(descProto, mvccTimestamp)
	if b == nil {
		return nil, nil
	}
	if err := b.RunPostDeserializationChanges(); err != nil {
		return nil, err
	}
	return b, nil
}

func newBuilder(
	descProto *descpb.Descriptor, mvccTimestamp hlc.Timestamp,
) catalog.DescriptorBuilder {
	switch t := descProto.Union.(type) {
	case *descpb.Descriptor_Table:
		return tabledesc.NewBuilderWithMVCCTimestamp(t.Table, mvccTimestamp)
	case *descpb.Descriptor_Database:
		return dbdesc.NewBuilderWithMVCCTimestamp(t.Database, mvccTimestamp)
	case *descpb.Descriptor_Type:
		return typedesc.NewBuilderWithMVCCTimestamp(t.Type, mvccTimestamp)
	case *descpb.Descriptor_Schema:
		return schemadesc.NewBuilderWithMVCCTimestamp(t.Schema, mvccTimestamp)
	case *descpb.Descriptor_Function:
		return funcdesc.NewBuilderWithMVCCTimestamp(t.Function, mvccTimestamp)
	}
	return nil
}

// BuildMutable generates a catalog.MutableDescriptor from scratch based on:
//   - the original descriptor (optional),
//   - the mutated descriptor,
//   - an MVCC timestamp (optional) to set the mutated descriptor's
//     ModificationTime with.
//
// The ID and Version fields may also be overwritten for correctness in case
// they deviate from what's expected.
func BuildMutable(
	original catalog.Descriptor, mutated *descpb.Descriptor, mvccTimestamp hlc.Timestamp,
) (catalog.MutableDescriptor, error) {
	if mutated == nil {
		return nil, errors.AssertionFailedf("empty mutated descriptor")
	}
	pb := protoutil.Clone(mutated).(*descpb.Descriptor)
	// Under no circumstances can the mutated ID be any different than the
	// original ID (if there is one). Similarly, the version should be the same
	// until it's bumped by a later call to MaybeIncrementVersion. Furthermore,
	// ModificationTime should not remain unset if an MVCC timestamp is available.
	correctFieldValues := func(newID *descpb.ID, newVersion *descpb.DescriptorVersion, newModTime *hlc.Timestamp) {
		if original != nil && *newID != original.GetID() {
			*newID = original.GetID()
		}
		if original == nil {
			*newVersion = 1
		} else if *newVersion != original.GetVersion() {
			*newVersion = original.GetVersion()
		}
		if !mvccTimestamp.IsEmpty() && newModTime.IsEmpty() {
			*newModTime = mvccTimestamp
		}
	}
	var actualType catalog.DescriptorType
	switch t := pb.Union.(type) {
	case *descpb.Descriptor_Table:
		correctFieldValues(&t.Table.ID, &t.Table.Version, &t.Table.ModificationTime)
		actualType = catalog.Table
	case *descpb.Descriptor_Database:
		correctFieldValues(&t.Database.ID, &t.Database.Version, &t.Database.ModificationTime)
		actualType = catalog.Database
	case *descpb.Descriptor_Type:
		correctFieldValues(&t.Type.ID, &t.Type.Version, &t.Type.ModificationTime)
		actualType = catalog.Type
	case *descpb.Descriptor_Schema:
		correctFieldValues(&t.Schema.ID, &t.Schema.Version, &t.Schema.ModificationTime)
		actualType = catalog.Schema
	case *descpb.Descriptor_Function:
		correctFieldValues(&t.Function.ID, &t.Function.Version, &t.Function.ModificationTime)
		actualType = catalog.Function
	default:
		return nil, errors.AssertionFailedf("invalid mutated descriptor: %+v", mutated)
	}
	if original == nil {
		return newBuilder(pb, hlc.Timestamp{}).BuildCreatedMutable(), nil
	}
	if original.DescriptorType() != actualType {
		return nil, errors.AssertionFailedf(
			"expected mutated %s, instead got a %s", original.DescriptorType(), actualType,
		)
	}
	ret := original.NewBuilder().BuildExistingMutable()
	tbl, db, typ, sc, fn := descpb.GetDescriptors(pb)
	switch m := ret.(type) {
	case *tabledesc.Mutable:
		m.TableDescriptor = *tbl
	case *dbdesc.Mutable:
		m.DatabaseDescriptor = *db
	case *typedesc.Mutable:
		m.TypeDescriptor = *typ
	case *schemadesc.Mutable:
		m.SchemaDescriptor = *sc
	case *funcdesc.Mutable:
		m.FunctionDescriptor = *fn
	}
	return ret, nil
}
