// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"
	"runtime/debug"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Descriptor is an interface to be shared by individual descriptor
// types.
type Descriptor interface {
	tree.NameResolutionResult

	GetID() descpb.ID
	GetName() string
	GetParentID() descpb.ID
	GetParentSchemaID() descpb.ID

	// Metadata for descriptor leasing.
	GetVersion() descpb.DescriptorVersion
	GetModificationTime() hlc.Timestamp
	GetDrainingNames() []descpb.NameInfo

	GetPrivileges() *descpb.PrivilegeDescriptor
	TypeName() string
	GetAuditMode() descpb.TableDescriptor_AuditMode

	Adding() bool
	// Note: Implementers of Dropped() should also update the implementation
	// (*descpb.Descriptor).Dropped(). These implementations are not shared
	// behind this interface.
	Dropped() bool
	Offline() bool
	GetOfflineReason() string

	// DescriptorProto prepares this descriptor for serialization.
	DescriptorProto() *descpb.Descriptor
}

// GetDescriptorMetadata extracts metadata out of a raw descpb.Descriptor.
func GetDescriptorMetadata(
	desc *descpb.Descriptor,
) (id descpb.ID, version descpb.DescriptorVersion, name string, offline, dropped bool) {
	return GetDescriptorID(desc), GetDescriptorVersion(desc), GetDescriptorName(desc),
		GetDescriptorDropped(desc), GetDescriptorOffline(desc)
}

// GetDescriptorID returns the ID of the descriptor.
func GetDescriptorID(desc *descpb.Descriptor) descpb.ID {
	switch t := desc.Union.(type) {
	case *descpb.Descriptor_Table:
		return t.Table.ID
	case *descpb.Descriptor_Database:
		return t.Database.ID
	case *descpb.Descriptor_Type:
		return t.Type.ID
	case *descpb.Descriptor_Schema:
		return t.Schema.ID
	default:
		panic(errors.AssertionFailedf("GetID: unknown Descriptor type %T", t))
	}
}

// GetDescriptorName returns the Name of the descriptor.
func GetDescriptorName(desc *descpb.Descriptor) string {
	switch t := desc.Union.(type) {
	case *descpb.Descriptor_Table:
		return t.Table.Name
	case *descpb.Descriptor_Database:
		return t.Database.Name
	case *descpb.Descriptor_Type:
		return t.Type.Name
	case *descpb.Descriptor_Schema:
		return t.Schema.Name
	default:
		panic(errors.AssertionFailedf("GetDescriptorName: unknown Descriptor type %T", t))
	}
}

// GetDescriptorVersion returns the Version of the descriptor.
func GetDescriptorVersion(desc *descpb.Descriptor) descpb.DescriptorVersion {
	switch t := desc.Union.(type) {
	case *descpb.Descriptor_Table:
		return t.Table.Version
	case *descpb.Descriptor_Database:
		return t.Database.Version
	case *descpb.Descriptor_Type:
		return t.Type.Version
	case *descpb.Descriptor_Schema:
		return t.Schema.Version
	default:
		panic(errors.AssertionFailedf("GetVersion: unknown Descriptor type %T", t))
	}
}

// GetDescriptorModificationTime returns the ModificationTime of the descriptor.
func GetDescriptorModificationTime(desc *descpb.Descriptor) hlc.Timestamp {
	switch t := desc.Union.(type) {
	case *descpb.Descriptor_Table:
		return t.Table.ModificationTime
	case *descpb.Descriptor_Database:
		return t.Database.ModificationTime
	case *descpb.Descriptor_Type:
		return t.Type.ModificationTime
	case *descpb.Descriptor_Schema:
		return t.Schema.ModificationTime
	default:
		debug.PrintStack()
		panic(errors.AssertionFailedf("GetDescriptorModificationTime: unknown Descriptor type %T", t))
	}
}

// GetDescriptorDropped returns whether the descriptor is dropped.
// TODO (lucy): Does this method belong on Descriptor? This state does matter
// for descriptor leasing, but arguably we should be upwrapping the descriptor
// to get it.
func GetDescriptorDropped(desc *descpb.Descriptor) bool {
	switch t := desc.Union.(type) {
	case *descpb.Descriptor_Table:
		return t.Table.Dropped()
	case *descpb.Descriptor_Type:
		return t.Type.Dropped()
	case *descpb.Descriptor_Schema:
		return t.Schema.Dropped()
	case *descpb.Descriptor_Database:
		return t.Database.Dropped()
	default:
		debug.PrintStack()
		panic(errors.AssertionFailedf("Dropped: unknown Descriptor type %T", t))
	}
}

// GetDescriptorOffline returns whether the descriptor is offline.
// TODO (lucy): Does this method belong on Descriptor? This state does matter
// for descriptor leasing, but arguably we should be upwrapping the descriptor
// to get it.
func GetDescriptorOffline(desc *descpb.Descriptor) bool {
	switch t := desc.Union.(type) {
	case *descpb.Descriptor_Table:
		return t.Table.Offline()
	case *descpb.Descriptor_Database, *descpb.Descriptor_Type, *descpb.Descriptor_Schema:
		return false
	default:
		debug.PrintStack()
		panic(errors.AssertionFailedf("Offline: unknown Descriptor type %T", t))
	}
}

// setDescriptorModificationTime sets the ModificationTime of the descriptor.
func setDescriptorModificationTime(desc *descpb.Descriptor, ts hlc.Timestamp) {
	switch t := desc.Union.(type) {
	case *descpb.Descriptor_Table:
		t.Table.ModificationTime = ts
	case *descpb.Descriptor_Database:
		t.Database.ModificationTime = ts
	case *descpb.Descriptor_Type:
		t.Type.ModificationTime = ts
	case *descpb.Descriptor_Schema:
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
func MaybeSetDescriptorModificationTimeFromMVCCTimestamp(
	ctx context.Context, desc *descpb.Descriptor, ts hlc.Timestamp,
) {
	switch t := desc.Union.(type) {
	case nil:
		// Empty descriptors shouldn't be touched.
		return
	case *descpb.Descriptor_Table:
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

// TableFromDescriptor is a replacement for GetTable() which seeks to ensure
// that clients which unmarshal Descriptor structs properly set the
// ModificationTime on tables based on the MVCC timestamp at which the
// descriptor was read.
//
// A linter should ensure that GetTable() is not called.
//
// TODO(ajwerner): Now that all descriptors have their modification time set
// this way, this function should be retired and similar or better safeguards
// for all descriptors should be pursued.
func TableFromDescriptor(desc *descpb.Descriptor, ts hlc.Timestamp) *descpb.TableDescriptor {
	t := desc.GetTable()
	if t != nil {
		MaybeSetDescriptorModificationTimeFromMVCCTimestamp(context.TODO(), desc, ts)
	}
	return t
}
