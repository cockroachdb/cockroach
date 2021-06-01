// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlerrors exports errors which can occur in the sql package.
package sqlerrors

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

const (
	txnAbortedMsg = "current transaction is aborted, commands ignored " +
		"until end of transaction block"
	txnCommittedMsg = "current transaction is committed, commands ignored " +
		"until end of transaction block"
)

// NewTransactionAbortedError creates an error for trying to run a command in
// the context of transaction that's in the aborted state. Any statement other
// than ROLLBACK TO SAVEPOINT will return this error.
func NewTransactionAbortedError(customMsg string) error {
	if customMsg != "" {
		return pgerror.Newf(
			pgcode.InFailedSQLTransaction, "%s: %s", customMsg, txnAbortedMsg)
	}
	return pgerror.New(pgcode.InFailedSQLTransaction, txnAbortedMsg)
}

// NewTransactionCommittedError creates an error that signals that the SQL txn
// is in the COMMIT_WAIT state and that only a COMMIT statement will be accepted.
func NewTransactionCommittedError() error {
	return pgerror.New(pgcode.InvalidTransactionState, txnCommittedMsg)
}

// NewNonNullViolationError creates an error for a violation of a non-NULL constraint.
func NewNonNullViolationError(columnName string) error {
	return pgerror.Newf(pgcode.NotNullViolation, "null value in column %q violates not-null constraint", columnName)
}

// NewInvalidSchemaDefinitionError creates an error for an invalid schema
// definition such as a schema definition that doesn't parse.
func NewInvalidSchemaDefinitionError(err error) error {
	return pgerror.WithCandidateCode(err, pgcode.InvalidSchemaDefinition)
}

// NewUndefinedSchemaError creates an error for an undefined schema.
// TODO (lucy): Have this take a database name.
func NewUndefinedSchemaError(name string) error {
	return pgerror.Newf(pgcode.InvalidSchemaName, "unknown schema %q", name)
}

// NewCCLRequiredError creates an error for when a CCL feature is used in an OSS
// binary.
func NewCCLRequiredError(err error) error {
	return pgerror.WithCandidateCode(err, pgcode.CCLRequired)
}

// IsCCLRequiredError returns whether the error is a CCLRequired error.
func IsCCLRequiredError(err error) bool {
	return errHasCode(err, pgcode.CCLRequired)
}

// NewUndefinedDatabaseError creates an error that represents a missing database.
func NewUndefinedDatabaseError(name string) error {
	// Postgres will return an UndefinedTable error on queries that go to a "relation"
	// that does not exist (a query to a non-existent table or database), but will
	// return an InvalidCatalogName error when connecting to a database that does
	// not exist. We've chosen to return this code for all cases where the error cause
	// is a missing database.
	return pgerror.Newf(
		pgcode.InvalidCatalogName, "database %q does not exist", name)
}

// NewInvalidWildcardError creates an error that represents the result of expanding
// a table wildcard over an invalid database or schema prefix.
func NewInvalidWildcardError(name string) error {
	return pgerror.Newf(
		pgcode.InvalidCatalogName,
		"%q does not match any valid database or schema", name)
}

// NewUndefinedObjectError returns the correct undefined object error based on
// the kind of object that was requested.
func NewUndefinedObjectError(name tree.NodeFormatter, kind tree.DesiredObjectKind) error {
	switch kind {
	case tree.TableObject:
		return NewUndefinedRelationError(name)
	case tree.TypeObject:
		return NewUndefinedTypeError(name)
	default:
		return errors.AssertionFailedf("unknown object kind %d", kind)
	}
}

// NewUndefinedTypeError creates an error that represents a missing type.
func NewUndefinedTypeError(name tree.NodeFormatter) error {
	return pgerror.Newf(pgcode.UndefinedObject, "type %q does not exist", tree.ErrString(name))
}

// NewUndefinedRelationError creates an error that represents a missing database table or view.
func NewUndefinedRelationError(name tree.NodeFormatter) error {
	return pgerror.Newf(pgcode.UndefinedTable,
		"relation %q does not exist", tree.ErrString(name))
}

// NewColumnAlreadyExistsError creates an error for a preexisting column.
func NewColumnAlreadyExistsError(name, relation string) error {
	return pgerror.Newf(pgcode.DuplicateColumn, "column %q of relation %q already exists", name, relation)
}

// NewDatabaseAlreadyExistsError creates an error for a preexisting database.
func NewDatabaseAlreadyExistsError(name string) error {
	return pgerror.Newf(pgcode.DuplicateDatabase, "database %q already exists", name)
}

// NewSchemaAlreadyExistsError creates an error for a preexisting schema.
func NewSchemaAlreadyExistsError(name string) error {
	return pgerror.Newf(pgcode.DuplicateSchema, "schema %q already exists", name)
}

// WrapErrorWhileConstructingObjectAlreadyExistsErr is used to wrap an error
// when an error occurs while trying to get the colliding object for an
// ObjectAlreadyExistsErr.
func WrapErrorWhileConstructingObjectAlreadyExistsErr(err error) error {
	return pgerror.WithCandidateCode(errors.Wrap(err, "object already exists"), pgcode.DuplicateObject)
}

// MakeObjectAlreadyExistsError creates an error for a namespace collision
// with an arbitrary descriptor type.
func MakeObjectAlreadyExistsError(collidingObject *descpb.Descriptor, name string) error {
	switch collidingObject.Union.(type) {
	case *descpb.Descriptor_Table:
		return NewRelationAlreadyExistsError(name)
	case *descpb.Descriptor_Type:
		return NewTypeAlreadyExistsError(name)
	case *descpb.Descriptor_Database:
		return NewDatabaseAlreadyExistsError(name)
	case *descpb.Descriptor_Schema:
		return NewSchemaAlreadyExistsError(name)
	default:
		return errors.AssertionFailedf("unknown type %T exists with name %v", collidingObject.Union, name)
	}
}

// NewRelationAlreadyExistsError creates an error for a preexisting relation.
func NewRelationAlreadyExistsError(name string) error {
	return pgerror.Newf(pgcode.DuplicateRelation, "relation %q already exists", name)
}

// NewTypeAlreadyExistsError creates an error for a preexisting type.
func NewTypeAlreadyExistsError(name string) error {
	return pgerror.Newf(pgcode.DuplicateObject, "type %q already exists", name)
}

// IsRelationAlreadyExistsError checks whether this is an error for a preexisting relation.
func IsRelationAlreadyExistsError(err error) bool {
	return errHasCode(err, pgcode.DuplicateRelation)
}

// NewWrongObjectTypeError creates a wrong object type error.
func NewWrongObjectTypeError(name tree.NodeFormatter, desiredObjType string) error {
	return pgerror.Newf(pgcode.WrongObjectType, "%q is not a %s",
		tree.ErrString(name), desiredObjType)
}

// NewSyntaxErrorf creates a syntax error.
func NewSyntaxErrorf(format string, args ...interface{}) error {
	return pgerror.Newf(pgcode.Syntax, format, args...)
}

// NewDependentObjectErrorf creates a dependent object error.
func NewDependentObjectErrorf(format string, args ...interface{}) error {
	return pgerror.Newf(pgcode.DependentObjectsStillExist, format, args...)
}

// NewRangeUnavailableError creates an unavailable range error.
func NewRangeUnavailableError(rangeID roachpb.RangeID, origErr error) error {
	// TODO(knz): This could should really use errors.Wrap or
	// errors.WithSecondaryError.
	return pgerror.Newf(pgcode.RangeUnavailable,
		"key range id:%d is unavailable. Original error: %v",
		rangeID, origErr)
}

// NewWindowInAggError creates an error for the case when a window function is
// nested within an aggregate function.
func NewWindowInAggError() error {
	return pgerror.New(pgcode.Grouping,
		"window functions are not allowed in aggregate")
}

// NewAggInAggError creates an error for the case when an aggregate function is
// contained within another aggregate function.
func NewAggInAggError() error {
	return pgerror.New(pgcode.Grouping, "aggregate function calls cannot be nested")
}

// QueryTimeoutError is an error representing a query timeout.
var QueryTimeoutError = pgerror.New(
	pgcode.QueryCanceled, "query execution canceled due to statement timeout")

// IsOutOfMemoryError checks whether this is an out of memory error.
func IsOutOfMemoryError(err error) bool {
	return errHasCode(err, pgcode.OutOfMemory)
}

// IsDiskFullError checks whether this is a disk full error.
func IsDiskFullError(err error) bool {
	return errHasCode(err, pgcode.DiskFull)
}

// IsUndefinedColumnError checks whether this is an undefined column error.
func IsUndefinedColumnError(err error) bool {
	return errHasCode(err, pgcode.UndefinedColumn)
}

// IsUndefinedRelationError checks whether this is an undefined relation error.
func IsUndefinedRelationError(err error) bool {
	return errHasCode(err, pgcode.UndefinedTable)
}

func errHasCode(err error, code ...pgcode.Code) bool {
	pgCode := pgerror.GetPGCode(err)
	for _, c := range code {
		if pgCode == c {
			return true
		}
	}
	return false
}
