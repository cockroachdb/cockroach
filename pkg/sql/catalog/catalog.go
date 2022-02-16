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
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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
	//
	// TODO(postamar): remove SetDrainingNames method in 22.2
	SetDrainingNames([]descpb.NameInfo) // Deprecated
	// AddDrainingName adds a draining name to the descriptor.
	//
	// TODO(postamar): remove AddDrainingName method in 22.2
	AddDrainingName(descpb.NameInfo) // Deprecated

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

	// SetDeclarativeSchemaChangerState sets the state of the declarative
	// schema change currently operating on this descriptor.
	SetDeclarativeSchemaChangerState(*scpb.DescriptorState)
}

// VirtualSchemas is a collection of VirtualSchemas.
type VirtualSchemas interface {
	GetVirtualSchema(schemaName string) (VirtualSchema, bool)
	GetVirtualSchemaByID(id descpb.ID) (VirtualSchema, bool)
	GetVirtualObjectByID(id descpb.ID) (VirtualObject, bool)
}

// VirtualSchema represents a collection of VirtualObjects.
type VirtualSchema interface {
	Desc() SchemaDescriptor
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
	// ExplicitDatabase and ExplicitSchema configure what is returned
	// in the NamePrefix call.
	ExplicitDatabase, ExplicitSchema bool

	// Database is the parent database descriptor.
	Database DatabaseDescriptor
	// Schema is the parent schema.
	Schema SchemaDescriptor
}

// NamePrefix returns the tree.ObjectNamePrefix with the appropriate names
// and indications about which of those names were provided explicitly.
func (p ResolvedObjectPrefix) NamePrefix() tree.ObjectNamePrefix {
	var n tree.ObjectNamePrefix
	n.ExplicitCatalog = p.ExplicitDatabase
	n.ExplicitSchema = p.ExplicitSchema
	if p.Database != nil {
		n.CatalogName = tree.Name(p.Database.GetName())
	}
	if p.Schema != nil {
		n.SchemaName = tree.Name(p.Schema.GetName())
	}
	return n
}

// NumSystemColumns defines the number of supported system columns and must be
// equal to colinfo.numSystemColumns (enforced in colinfo package to avoid an
// import cycle).
const NumSystemColumns = 2

// SmallestSystemColumnColumnID is a descpb.ColumnID with the smallest value
// among all system columns (enforced in colinfo package to avoid an import
// cycle).
const SmallestSystemColumnColumnID = math.MaxUint32 - NumSystemColumns + 1
