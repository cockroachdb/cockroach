// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ValidateName validates a name.
func ValidateName(name, typ string) error {
	if len(name) == 0 {
		return pgerror.Newf(pgcode.Syntax, "empty %s name", typ)
	}
	// TODO(pmattis): Do we want to be more restrictive than this?
	return nil
}

type inactiveDescriptorError struct {
	cause error
}

// errTableAdding is returned when the descriptor is being added.
//
// Only tables (or materialized view) can be in the adding state, and this will
// be true for the foreseeable future, so the error message remains a
// table-specific version.
type addingTableError struct {
	cause error
}

func newAddingTableError(desc TableDescriptor) error {
	typStr := "table"
	if desc.IsView() && desc.IsPhysicalTable() {
		typStr = "materialized view"
	}
	return &addingTableError{
		cause: errors.Errorf("%s %q is being added", typStr, desc.GetName()),
	}
}

func (a *addingTableError) Error() string { return a.cause.Error() }

func (a *addingTableError) Unwrap() error { return a.cause }

// ErrDescriptorDropped is returned when the descriptor is being dropped.
// TODO (lucy): Make the error message specific to each descriptor type (e.g.,
// "table is being dropped") and add the pgcodes (UndefinedTable, etc.).
var ErrDescriptorDropped = errors.New("descriptor is being dropped")

func (i *inactiveDescriptorError) Error() string { return i.cause.Error() }

func (i *inactiveDescriptorError) Unwrap() error { return i.cause }

// HasAddingTableError returns true if the error contains errTableAdding.
func HasAddingTableError(err error) bool {
	return errors.HasType(err, (*addingTableError)(nil))
}

// NewInactiveDescriptorError wraps an error in a new inactiveDescriptorError.
func NewInactiveDescriptorError(err error) error {
	return &inactiveDescriptorError{err}
}

// HasInactiveDescriptorError returns true if the error contains an
// inactiveDescriptorError.
func HasInactiveDescriptorError(err error) bool {
	return errors.HasType(err, (*inactiveDescriptorError)(nil))
}

// ErrDescriptorNotFound is returned to signal that a descriptor could not be
// found with the given id.
var ErrDescriptorNotFound = errors.New("descriptor not found")

// ErrDescriptorWrongType is returned to signal that a descriptor was found but
// that it wasn't of the expected type.
var ErrDescriptorWrongType = errors.New("unexpected descriptor type")

// NewDescriptorTypeError returns ErrDescriptorWrongType prefixed with
// the actual go type of the descriptor.
func NewDescriptorTypeError(desc interface{}) error {
	return errors.Wrapf(ErrDescriptorWrongType, "descriptor is a %T", desc)
}

// AsDatabaseDescriptor tries to cast desc to a DatabaseDescriptor.
// Returns an ErrDescriptorWrongType otherwise.
func AsDatabaseDescriptor(desc Descriptor) (DatabaseDescriptor, error) {
	db, ok := desc.(DatabaseDescriptor)
	if !ok {
		if desc == nil {
			return nil, NewDescriptorTypeError(desc)
		}
		return nil, WrapDatabaseDescRefErr(desc.GetID(), NewDescriptorTypeError(desc))
	}
	return db, nil
}

// AsSchemaDescriptor tries to cast desc to a SchemaDescriptor.
// Returns an ErrDescriptorWrongType otherwise.
func AsSchemaDescriptor(desc Descriptor) (SchemaDescriptor, error) {
	schema, ok := desc.(SchemaDescriptor)
	if !ok {
		if desc == nil {
			return nil, NewDescriptorTypeError(desc)
		}
		return nil, WrapSchemaDescRefErr(desc.GetID(), NewDescriptorTypeError(desc))
	}
	return schema, nil
}

// AsTableDescriptor tries to cast desc to a TableDescriptor.
// Returns an ErrDescriptorWrongType otherwise.
func AsTableDescriptor(desc Descriptor) (TableDescriptor, error) {
	table, ok := desc.(TableDescriptor)
	if !ok {
		if desc == nil {
			return nil, NewDescriptorTypeError(desc)
		}
		return nil, WrapTableDescRefErr(desc.GetID(), NewDescriptorTypeError(desc))
	}
	return table, nil
}

// AsTypeDescriptor tries to cast desc to a TypeDescriptor.
// Returns an ErrDescriptorWrongType otherwise.
func AsTypeDescriptor(desc Descriptor) (TypeDescriptor, error) {
	typ, ok := desc.(TypeDescriptor)
	if !ok {
		if desc == nil {
			return nil, NewDescriptorTypeError(desc)
		}
		return nil, WrapTypeDescRefErr(desc.GetID(), NewDescriptorTypeError(desc))
	}
	return typ, nil
}

// WrapDatabaseDescRefErr wraps an error pertaining to a database descriptor id.
func WrapDatabaseDescRefErr(id descpb.ID, err error) error {
	return errors.Wrapf(err, "referenced database ID %d", errors.Safe(id))
}

// WrapSchemaDescRefErr wraps an error pertaining to a schema descriptor id.
func WrapSchemaDescRefErr(id descpb.ID, err error) error {
	return errors.Wrapf(err, "referenced schema ID %d", errors.Safe(id))
}

// WrapTableDescRefErr wraps an error pertaining to a table descriptor id.
func WrapTableDescRefErr(id descpb.ID, err error) error {
	return errors.Wrapf(err, "referenced table ID %d", errors.Safe(id))
}

// WrapTypeDescRefErr wraps an error pertaining to a type descriptor id.
func WrapTypeDescRefErr(id descpb.ID, err error) error {
	return errors.Wrapf(err, "referenced type ID %d", errors.Safe(id))
}

// NewMutableAccessToVirtualSchemaError is returned when trying to mutably
// access a virtual schema object.
func NewMutableAccessToVirtualSchemaError(entry VirtualSchema, object string) error {
	switch entry.Desc().GetName() {
	case "pg_catalog":
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a system catalog", tree.ErrNameString(object))
	default:
		return pgerror.Newf(pgcode.WrongObjectType,
			"%s is a virtual object and cannot be modified", tree.ErrNameString(object))
	}
}
