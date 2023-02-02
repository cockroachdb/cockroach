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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// SchemaDescriptor encapsulates the basic
type SchemaDescriptor interface {
	Descriptor

	// SchemaKind indicates whether this descriptor
	SchemaKind() ResolvedSchemaKind

	// SchemaDesc returns the underlying protocol buffer in the
	// case that this is a real descriptor.
	SchemaDesc() *descpb.SchemaDescriptor

	// GetDefaultPrivilegeDescriptor returns the default privileges for this
	// database.
	GetDefaultPrivilegeDescriptor() DefaultPrivilegeDescriptor

	// GetFunction returns a list of function overloads given a name.
	GetFunction(name string) (descpb.SchemaDescriptor_Function, bool)

	// GetResolvedFuncDefinition returns a ResolvedFunctionDefinition given a
	// function name. This is needed by function resolution and expression type
	// checking during which candidate function overloads are searched for the
	// best match. Only function signatures are needed during this process. Schema
	// stores all the signatures of the functions created under it and this method
	// returns a collection of overloads with the same function name, each
	// overload is prefixed with the same schema name.
	GetResolvedFuncDefinition(name string) (*tree.ResolvedFunctionDefinition, bool)

	// ForEachFunctionSignature iterates through all function signatures within
	// the schema and calls fn on each signature.
	ForEachFunctionSignature(fn func(sig descpb.SchemaDescriptor_FunctionSignature) error) error
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
