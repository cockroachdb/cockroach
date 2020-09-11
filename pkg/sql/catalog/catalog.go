// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// MutableDescriptor represents a descriptor undergoing in-memory mutations
// as part of a schema change.
type MutableDescriptor interface {
	Descriptor
	// MaybeIncrementVersion sets the version of the descriptor to
	// OriginalVersion()+1.
	// TODO (lucy): It's not a good idea to have callers handle incrementing the
	// version manually. Find a better API for this. Maybe creating a new mutable
	// descriptor should increment the version on the mutable copy from the
	// outset.
	MaybeIncrementVersion()
	// SetDrainingNames sets the draining names for the descriptor.
	SetDrainingNames([]descpb.NameInfo)

	// Accessors for the original state of the descriptor prior to the mutations.
	OriginalName() string
	OriginalID() descpb.ID
	OriginalVersion() descpb.DescriptorVersion
	// ImmutableCopy returns an immutable copy of this descriptor.
	ImmutableCopy() Descriptor
	// IsNew returns whether the descriptor was created in this transaction.
	IsNew() bool

	// SetPublic sets the descriptor's state to public.
	SetPublic()
	// SetDropped sets the descriptor's state to dropped.
	SetDropped()
	// SetOffline sets the descriptor's state to offline, with the provided reason.
	SetOffline(reason string)
}

// VirtualSchemas is a collection of VirtualSchemas.
type VirtualSchemas interface {
	GetVirtualSchema(schemaName string) (VirtualSchema, bool)
}

// VirtualSchema represents a collection of VirtualObjects.
type VirtualSchema interface {
	Desc() Descriptor
	NumTables() int
	VisitTables(func(object VirtualObject))
	GetObjectByName(name string, flags tree.ObjectLookupFlags) (VirtualObject, error)
}

// VirtualObject is a virtual schema object.
type VirtualObject interface {
	Desc() Descriptor
}

// ResolvedObjectPrefix represents the resolved components of an object name
// prefix. It contains the parent database and schema.
type ResolvedObjectPrefix struct {
	// Database is the parent database descriptor.
	Database DatabaseDescriptor
	// Schema is the parent schema.
	Schema ResolvedSchema
}

// SchemaMeta implements the SchemaMeta interface.
func (*ResolvedObjectPrefix) SchemaMeta() {}

// ResolvedSchemaKind is an enum that represents what kind of schema
// has been resolved.
type ResolvedSchemaKind int

const (
	// SchemaPublic represents the public schema.
	SchemaPublic ResolvedSchemaKind = iota
	// SchemaVirtual represents a virtual schema.
	SchemaVirtual
	// SchemaTemporary represents a temporary schema.
	SchemaTemporary
	// SchemaUserDefined represents a user defined schema.
	SchemaUserDefined
)

// ResolvedSchema represents the result of resolving a schema name, or an
// object prefix of <db>.<schema>. Due to historical reasons, some schemas
// don't have unique IDs (public and virtual schemas), and others aren't backed
// by descriptors. The ResolvedSchema struct encapsulates the different cases.
type ResolvedSchema struct {
	// Marks what kind of schema this is. It is always set.
	Kind ResolvedSchemaKind
	// Name of the resolved schema. It is always set.
	Name string
	// The ID of the resolved schema. This field is only set for schema kinds
	// SchemaPublic, SchemaUserDefined and SchemaTemporary.
	ID descpb.ID
	// The descriptor backing the resolved schema. It is only set for
	// SchemaUserDefined.
	Desc SchemaDescriptor
}
