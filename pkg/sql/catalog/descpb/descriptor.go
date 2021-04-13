// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GetDescriptorMetadata extracts metadata out of a raw descpb.Descriptor. Used
// in cases where basic type-agnostic metadata is needed and unwrapping the
// descriptor is unnecessary.
func GetDescriptorMetadata(
	desc *Descriptor,
) (
	id ID,
	version DescriptorVersion,
	name string,
	state DescriptorState,
	modTime hlc.Timestamp,
	err error,
) {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		id = t.Table.ID
		version = t.Table.Version
		name = t.Table.Name
		state = t.Table.State
		modTime = t.Table.ModificationTime
	case *Descriptor_Database:
		id = t.Database.ID
		version = t.Database.Version
		name = t.Database.Name
		state = t.Database.State
		modTime = t.Database.ModificationTime
	case *Descriptor_Type:
		id = t.Type.ID
		version = t.Type.Version
		name = t.Type.Name
		state = t.Type.State
		modTime = t.Type.ModificationTime
	case *Descriptor_Schema:
		id = t.Schema.ID
		version = t.Schema.Version
		name = t.Schema.Name
		state = t.Schema.State
		modTime = t.Schema.ModificationTime
	case nil:
		err = errors.AssertionFailedf("Table/Database/Type/Schema not set in descpb.Descriptor")
	default:
		err = errors.AssertionFailedf("Unknown descpb.Descriptor type %T", t)
	}
	return id, version, name, state, modTime, err
}

// GetDescriptorID returns the ID of the descriptor.
func GetDescriptorID(desc *Descriptor) ID {
	id, _, _, _, _, err := GetDescriptorMetadata(desc)
	if err != nil {
		panic(errors.Wrap(err, "GetDescriptorID"))
	}
	return id
}

// GetDescriptorName returns the Name of the descriptor.
func GetDescriptorName(desc *Descriptor) string {
	_, _, name, _, _, err := GetDescriptorMetadata(desc)
	if err != nil {
		panic(errors.Wrap(err, "GetDescriptorName"))
	}
	return name
}

// GetDescriptorVersion returns the Version of the descriptor.
func GetDescriptorVersion(desc *Descriptor) DescriptorVersion {
	_, version, _, _, _, err := GetDescriptorMetadata(desc)
	if err != nil {
		panic(errors.Wrap(err, "GetDescriptorVersion"))
	}
	return version
}

// GetDescriptorModificationTime returns the ModificationTime of the descriptor.
func GetDescriptorModificationTime(desc *Descriptor) hlc.Timestamp {
	_, _, _, _, modTime, err := GetDescriptorMetadata(desc)
	if err != nil {
		panic(errors.Wrap(err, "GetDescriptorModificationTime"))
	}
	return modTime
}

// GetDescriptorState returns the DescriptorState of the Descriptor.
func GetDescriptorState(desc *Descriptor) DescriptorState {
	_, _, _, state, _, err := GetDescriptorMetadata(desc)
	if err != nil {
		panic(errors.Wrap(err, "GetDescriptorState"))
	}
	return state
}

// setDescriptorModificationTime sets the ModificationTime of the descriptor.
func setDescriptorModificationTime(desc *Descriptor, ts hlc.Timestamp) {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		t.Table.ModificationTime = ts
	case *Descriptor_Database:
		t.Database.ModificationTime = ts
	case *Descriptor_Type:
		t.Type.ModificationTime = ts
	case *Descriptor_Schema:
		t.Schema.ModificationTime = ts
	default:
		panic(errors.AssertionFailedf("setModificationTime: unknown Descriptor type %T", t))
	}
}

// MaybeSetDescriptorModificationTimeFromMVCCTimestamp will update
// ModificationTime and possibly CreateAsOfTime on TableDescriptor with the
// provided timestamp. If ModificationTime is non-zero it must be the case that
// it is not after the provided timestamp.
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
//
// It is vital that users which read table descriptor values from the KV store
// call this method.
func MaybeSetDescriptorModificationTimeFromMVCCTimestamp(desc *Descriptor, ts hlc.Timestamp) {
	switch t := desc.Union.(type) {
	case nil:
		// Empty descriptors shouldn't be touched.
		return
	case *Descriptor_Table:
		// CreateAsOfTime is used for CREATE TABLE ... AS ... and was introduced in
		// v19.1. In general it is not critical to set except for tables in the ADD
		// state which were created from CTAS so we should not assert on its not
		// being set. It's not always sensical to set it from the passed MVCC
		// timestamp. However, starting in 19.2 the CreateAsOfTime and
		// ModificationTime fields are both unset for the first Version of a
		// TableDescriptor and the code relies on the value being set based on the
		// MVCC timestamp.
		if !ts.IsEmpty() &&
			t.Table.ModificationTime.IsEmpty() &&
			t.Table.CreateAsOfTime.IsEmpty() &&
			t.Table.Version == 1 {
			t.Table.CreateAsOfTime = ts
		}

		// Ensure that if the table is in the process of being added and relies on
		// CreateAsOfTime that it is now set.
		if t.Table.Adding() && t.Table.IsAs() && t.Table.CreateAsOfTime.IsEmpty() {
			log.Fatalf(context.TODO(), "table descriptor for %q (%d.%d) is in the "+
				"ADD state and was created with CREATE TABLE ... AS but does not have a "+
				"CreateAsOfTime set", t.Table.Name, t.Table.ParentID, t.Table.ID)
		}
	}
	// Set the ModificationTime based on the passed ts if we should.
	// Table descriptors can be updated in place after their version has been
	// incremented (e.g. to include a schema change lease).
	// When this happens we permit the ModificationTime to be written explicitly
	// with the value that lives on the in-memory copy. That value should contain
	// a timestamp set by this method. Thus if the ModificationTime is set it
	// must not be after the MVCC timestamp we just read it at.
	if modTime := GetDescriptorModificationTime(desc); modTime.IsEmpty() && ts.IsEmpty() && GetDescriptorVersion(desc) > 1 {
		// TODO(ajwerner): reconsider the third condition here.It seems that there
		// are some cases where system tables lack this timestamp and then when they
		// are rendered in some other downstream setting we expect the timestamp to
		// be read. This is a hack we shouldn't need to do.
		log.Fatalf(context.TODO(), "read table descriptor for %q (%d) without ModificationTime "+
			"with zero MVCC timestamp; full descriptor:\n%s", GetDescriptorName(desc), GetDescriptorID(desc), desc)
	} else if modTime.IsEmpty() {
		setDescriptorModificationTime(desc, ts)
	} else if !ts.IsEmpty() && ts.Less(modTime) {
		log.Fatalf(context.TODO(), "read table descriptor %q (%d) which has a ModificationTime "+
			"after its MVCC timestamp: has %v, expected %v",
			GetDescriptorName(desc), GetDescriptorID(desc), modTime, ts)
	}
}

// FromDescriptorWithMVCCTimestamp is a replacement for
// Get(Table|Database|Type|Schema)() methods which seeks to ensure that clients
// which unmarshal Descriptor structs properly set the ModificationTime based on
// the MVCC timestamp at which the descriptor was read.
//
// A linter check ensures that GetTable() et al. are not called elsewhere unless
// absolutely necessary.
func FromDescriptorWithMVCCTimestamp(
	desc *Descriptor, ts hlc.Timestamp,
) (
	table *TableDescriptor,
	database *DatabaseDescriptor,
	typ *TypeDescriptor,
	schema *SchemaDescriptor,
) {
	if desc == nil {
		return nil, nil, nil, nil
	}
	//nolint:descriptormarshal
	table = desc.GetTable()
	//nolint:descriptormarshal
	database = desc.GetDatabase()
	//nolint:descriptormarshal
	typ = desc.GetType()
	//nolint:descriptormarshal
	schema = desc.GetSchema()
	MaybeSetDescriptorModificationTimeFromMVCCTimestamp(desc, ts)
	return table, database, typ, schema
}

// FromDescriptor is a convenience function for FromDescriptorWithMVCCTimestamp
// called with an empty timestamp. As a result this does not modify the
// descriptor.
func FromDescriptor(
	desc *Descriptor,
) (*TableDescriptor, *DatabaseDescriptor, *TypeDescriptor, *SchemaDescriptor) {
	return FromDescriptorWithMVCCTimestamp(desc, hlc.Timestamp{})
}
