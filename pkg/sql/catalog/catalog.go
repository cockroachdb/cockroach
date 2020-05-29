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
//
// This also includes an optional CheckHelper for the table (for CHECK
// constraints). This is needed for FK work because CASCADE actions
// can modify rows, and CHECK constraints must be applied to rows
// modified by CASCADE.
type TableEntry struct {
	// Desc is the descriptor of the table. This can be nil if eg.
	// the table is not public.
	Desc *sqlbase.ImmutableTableDescriptor

	// IsAdding indicates the descriptor is being created.
	IsAdding bool

	// CheckHelper is the utility responsible for CHECK constraint
	// checks. The lookup function (see TableLookupFunction below) needs
	// not populate this field; this is populated by the lookup queue
	// below.
	CheckHelper *sqlbase.CheckHelper
}
