// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// ValidateName validates a name.
func ValidateName(desc Descriptor) error {
	if len(desc.GetName()) > 0 {
		return nil
	}
	return pgerror.Newf(pgcode.Syntax, "empty %s name", desc.DescriptorType())
}

type inactiveDescriptorError struct {
	cause error
}

// errTableAdding is returned when the descriptor is being added.
//
// Only tables (or materialized view) can be in the adding state, and this will
// be true for the foreseeable future, so the error message remains a
// table-specific version.
type addingDescriptorError struct {
	cause error
}

func newAddingDescriptorError(desc Descriptor) error {
	var typStr string
	desc.DescriptorType()
	switch t := desc.(type) {
	case TableDescriptor:
		typStr = "table"
		if t.IsView() && t.IsPhysicalTable() {
			typStr = "materialized view"
		}
	default:
		typStr = string(desc.DescriptorType())
	}
	return &addingDescriptorError{
		cause: errors.Errorf("%s %q is being added", typStr, desc.GetName()),
	}
}

func (a *addingDescriptorError) Error() string { return a.cause.Error() }

func (a *addingDescriptorError) Unwrap() error { return a.cause }

// ErrDescriptorDropped is returned when the descriptor is being dropped.
// TODO (lucy): Make the error message specific to each descriptor type (e.g.,
// "table is being dropped") and add the pgcodes (UndefinedTable, etc.).
var ErrDescriptorDropped = errors.New("descriptor is being dropped")

func (i *inactiveDescriptorError) Error() string { return i.cause.Error() }

func (i *inactiveDescriptorError) Unwrap() error { return i.cause }

// HasAddingDescriptorError returns true if the error contains errTableAdding.
func HasAddingDescriptorError(err error) bool {
	return errors.HasType(err, (*addingDescriptorError)(nil))
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

func NewDescriptorNotFoundError(id descpb.ID) error {
	return errors.Wrapf(ErrDescriptorNotFound, "looking up ID %d", errors.Safe(id))
}

// ErrReferencedDescriptorNotFound is like ErrDescriptorNotFound but for
// descriptors referenced within another descriptor.
var ErrReferencedDescriptorNotFound = errors.New("referenced descriptor not found")

func NewReferencedDescriptorNotFoundError(descType string, id descpb.ID) error {
	return errors.Wrapf(ErrReferencedDescriptorNotFound, "referenced %s ID %d", redact.SafeString(descType), errors.Safe(id))
}

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

// AsFunctionDescriptor tries to case a descriptor to a FunctionDescriptor.
// Returns a
func AsFunctionDescriptor(desc Descriptor) (FunctionDescriptor, error) {
	typ, ok := desc.(FunctionDescriptor)
	if !ok {
		if desc == nil {
			return nil, NewDescriptorTypeError(desc)
		}
		return nil, WrapFunctionDescRefErr(desc.GetID(), NewDescriptorTypeError(desc))
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

// WrapFunctionDescRefErr wraps an error pertaining to a function descriptor id.
func WrapFunctionDescRefErr(id descpb.ID, err error) error {
	return errors.Wrapf(err, "referenced function ID %d", errors.Safe(id))
}

// NewMutableAccessToDescriptorlessSchemaError is returned when trying to mutably
// access a descriptorless schema object, including
// - descriptorless public schemas (deprecated),
// - temporary schemas,
// - virtual schemas (pg_catalog, pg_extension, information_schema, and crdb_internal)
func NewMutableAccessToDescriptorlessSchemaError(schema SchemaDescriptor) error {
	switch schema.SchemaKind() {
	case SchemaPublic:
		return pgerror.New(pgcode.InsufficientPrivilege,
			"descriptorless public schema cannot be modified")
	case SchemaTemporary:
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a temporary schema and cannot be modified", tree.ErrNameString(schema.GetName()))
	case SchemaVirtual:
		if schema.GetName() == "pg_catalog" {
			return pgerror.New(pgcode.InsufficientPrivilege, "pg_catalog is a system catalog")
		}
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a virtual schema and cannot be modified", tree.ErrNameString(schema.GetName()))
	}
	return errors.AssertionFailedf("schema %q (%d) of kind %d is not virtual",
		schema.GetName(), schema.GetID(), schema.SchemaKind())
}

// NewMutableAccessToVirtualObjectError is returned when trying to mutably
// access a virtual schema object.
func NewMutableAccessToVirtualObjectError(schema VirtualSchema, object VirtualObject) error {
	if schema == nil {
		return errors.AssertionFailedf("virtual object %q (%d) does not have a virtual parent schema",
			object.Desc().GetName(), object.Desc().GetID())
	}
	switch schema.Desc().GetName() {
	case "pg_catalog":
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is a system catalog", tree.ErrNameString(object.Desc().GetName()))
	default:
		return pgerror.Newf(pgcode.WrongObjectType,
			"%s is a virtual object and cannot be modified", tree.ErrNameString(object.Desc().GetName()))
	}
}
