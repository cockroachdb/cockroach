// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package descpb exposes the "Descriptor" type and related utilities.
package descpb

import (
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// GetDescriptorMetadata extracts metadata out of a raw descpb.Descriptor. Used
// in cases where basic type-agnostic metadata is needed and unwrapping the
// descriptor is unnecessary.
func GetDescriptorMetadata(
	desc *Descriptor,
) (id ID, version DescriptorVersion, name string, state DescriptorState, err error) {
	switch t := desc.Union.(type) {
	case *Descriptor_Table:
		id = t.Table.ID
		version = t.Table.Version
		name = t.Table.Name
		state = t.Table.State
	case *Descriptor_Database:
		id = t.Database.ID
		version = t.Database.Version
		name = t.Database.Name
		state = t.Database.State
	case *Descriptor_Type:
		id = t.Type.ID
		version = t.Type.Version
		name = t.Type.Name
		state = t.Type.State
	case *Descriptor_Schema:
		id = t.Schema.ID
		version = t.Schema.Version
		name = t.Schema.Name
		state = t.Schema.State
	case *Descriptor_Function:
		id = t.Function.ID
		version = t.Function.Version
		name = t.Function.Name
		state = t.Function.State
	case nil:
		err = errors.AssertionFailedf(
			"Table/Database/Type/Schema/Function not set in descpb.Descriptor")
	default:
		err = errors.AssertionFailedf("Unknown descpb.Descriptor type %T", t)
	}
	return id, version, name, state, err
}

// GetDescriptors is a replacement for Get* methods on descpb.Descriptor.
//
// A linter check ensures that GetTable() et al. are not called elsewhere unless
// absolutely necessary.
func GetDescriptors(
	desc *Descriptor,
) (
	table *TableDescriptor,
	database *DatabaseDescriptor,
	typ *TypeDescriptor,
	schema *SchemaDescriptor,
	function *FunctionDescriptor,
) {
	//nolint:descriptormarshal
	table = desc.GetTable()
	//nolint:descriptormarshal
	database = desc.GetDatabase()
	//nolint:descriptormarshal
	typ = desc.GetType()
	//nolint:descriptormarshal
	schema = desc.GetSchema()
	//nolint:descriptormarshal
	function = desc.GetFunction()
	return table, database, typ, schema, function
}

// MustSetModificationTime returns true iff a descriptor's ModificationTime
// field must be set to the given MVCC timestamp. An error is returned if the
// argument values are inconsistent.
func MustSetModificationTime(
	modTime hlc.Timestamp,
	mvccTimestamp hlc.Timestamp,
	version DescriptorVersion,
	state DescriptorState,
) (bool, error) {
	if state == DescriptorState_OFFLINE {
		return false, nil
	}
	// Set the ModificationTime based on the passed mvccTimestamp if we should.
	// Table descriptors can be updated in place after their version has been
	// incremented (e.g. to include a schema change lease).
	// When this happens we permit the ModificationTime to be written explicitly
	// with the value that lives on the in-memory copy. That value should contain
	// a timestamp set by this method. Thus if the ModificationTime is set it
	// must not be after the MVCC timestamp we just read it at.
	if mvccTimestamp.IsEmpty() {
		if modTime.IsEmpty() && version > 1 {
			return false, errors.AssertionFailedf(
				"modified existing descriptor requires ModificationTime to be set, " +
					"but provided MVCC timestamp is unset")
		}
		// Nothing to be done with an unset MVCC timestamp.
		return false, nil
	}
	if !modTime.IsEmpty() {
		if mvccTimestamp.Less(modTime) {
			return false, errors.AssertionFailedf(
				"cannot update descriptor ModificationTime %v with earlier MVCC timestamp %v",
				modTime, mvccTimestamp)
		}
		// ModificationTime is already set, nothing to do.
		// This supersedes the potential need to set CreateAsOfTime.
		return false, nil
	}
	return true, nil
}
