// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

// SchemaDescriptor encapsulates the basic
type SchemaDescriptor interface {
	Descriptor

	// SchemaKind indicates whether this descriptor
	SchemaKind() ResolvedSchemaKind

	// SchemaDesc returns the underlying protocol buffer in the
	// case that this is a real descriptor.
	SchemaDesc() *descpb.SchemaDescriptor
}

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
