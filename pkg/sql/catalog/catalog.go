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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Descriptor is an interface for retrieved catalog descriptors.
type Descriptor = sqlbase.DescriptorInterface

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
	SetDrainingNames([]sqlbase.NameInfo)

	// Accessors for the original state of the descriptor prior to the mutations.
	OriginalName() string
	OriginalID() sqlbase.ID
	OriginalVersion() sqlbase.DescriptorVersion
	// Immutable returns an immutable copy of this descriptor.
	Immutable() Descriptor
	// IsNew returns whether the descriptor was created in this transaction.
	IsNew() bool
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

// TableEntry is the value type of FkTableMetadata: An optional table
// descriptor, populated when the table is public/leasable, and an IsAdding
// flag.
type TableEntry struct {
	// Desc is the descriptor of the table. This can be nil if eg.
	// the table is not public.
	Desc *sqlbase.ImmutableTableDescriptor

	// IsAdding indicates the descriptor is being created.
	IsAdding bool
}

// ResolvedObjectPrefix represents the resolved components of an object name
// prefix. It contains the parent database and schema.
type ResolvedObjectPrefix struct {
	// Database is the parent database descriptor.
	Database *sqlbase.ImmutableDatabaseDescriptor
	// Schema is the parent schema.
	Schema sqlbase.ResolvedSchema
}

// SchemaMeta implements the SchemaMeta interface.
func (*ResolvedObjectPrefix) SchemaMeta() {}
