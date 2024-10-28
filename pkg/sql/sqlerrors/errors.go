// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package sqlerrors exports errors which can occur in the sql package.
package sqlerrors

import (
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	txnAbortedMsg = "current transaction is aborted, commands ignored " +
		"until end of transaction block"
	txnCommittedMsg = "current transaction is committed, commands ignored " +
		"until end of transaction block"
)

// EnforceHomeRegionFurtherInfo is the suffix to append to every error returned
// due to turning on the enforce_home_region session setting, and provides some
// information for users or app developers.
const EnforceHomeRegionFurtherInfo = "For more information, see https://www.cockroachlabs.com/docs/stable/cost-based-optimizer.html#control-whether-queries-are-limited-to-a-single-region"

// NewSchemaChangeOnLockedTableErr creates an error signaling schema
// change statement is attempted on a table with locked schema.
func NewSchemaChangeOnLockedTableErr(tableName string) error {
	return errors.WithHintf(pgerror.Newf(pgcode.OperatorIntervention,
		`schema changes are disallowed on table %q because it is locked`, tableName),
		"To unlock the table, try \"ALTER TABLE %v SET (schema_locked = false);\" "+
			"\nAfter schema change completes, we recommend setting it back to true with "+
			"\"ALTER TABLE %v SET (schema_locked = true);\"", tableName, tableName)
}

// NewDisallowedSchemaChangeOnLDRTableErr creates an error that indicates that
// the schema change is disallowed because the table is being used by a
// logical data replication job.
func NewDisallowedSchemaChangeOnLDRTableErr(tableName string, jobIDs []catpb.JobID) error {
	ids := make([]string, len(jobIDs))
	for i, v := range jobIDs {
		ids[i] = strconv.Itoa(int(v))
	}
	return pgerror.Newf(
		pgcode.FeatureNotSupported,
		"this schema change is disallowed on table %s because it is referenced by "+
			"one or more logical replication jobs [%s]", tableName, strings.Join(ids, ", "),
	)
}

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

func NewAlterColumnTypeColOwnsSequenceNotSupportedErr() error {
	return unimplemented.NewWithIssuef(
		48244, "ALTER COLUMN TYPE for a column that owns a sequence "+
			"is currently not supported")
}

func NewAlterColumnTypeColWithConstraintNotSupportedErr() error {
	return unimplemented.NewWithIssuef(
		48288, "ALTER COLUMN TYPE for a column that has a constraint "+
			"is currently not supported")
}

func NewAlterColumnTypeColInIndexNotSupportedErr() error {
	return unimplemented.NewWithIssuef(
		47636, "ALTER COLUMN TYPE requiring rewrite of on-disk "+
			"data is currently not supported for columns that are part of an index")
}

// NewInvalidAssignmentCastError creates an error that is used when a mutation
// cannot be performed because there is not a valid assignment cast from a
// value's type to the type of the target column.
func NewInvalidAssignmentCastError(
	sourceType *types.T, targetType *types.T, targetColName string,
) error {
	return errors.WithHint(
		pgerror.Newf(
			pgcode.DatatypeMismatch,
			"value type %s doesn't match type %s of column %q",
			sourceType, targetType, tree.ErrNameString(targetColName),
		),
		"you will need to rewrite or cast the expression",
	)
}

// NewGeneratedAlwaysAsIdentityColumnOverrideError creates an error for
// explicitly writing a column created with `GENERATED ALWAYS AS IDENTITY`
// syntax.
// TODO(janexing): Should add a HINT with "Use OVERRIDING SYSTEM VALUE
// to override." once issue #68201 is resolved.
// Check also: https://github.com/cockroachdb/cockroach/issues/68201.
func NewGeneratedAlwaysAsIdentityColumnOverrideError(columnName string) error {
	return errors.WithDetailf(
		pgerror.Newf(pgcode.GeneratedAlways, "cannot insert into column %q", columnName),
		"Column %q is an identity column defined as GENERATED ALWAYS", columnName,
	)
}

// NewGeneratedAlwaysAsIdentityColumnUpdateError creates an error for
// updating a column created with `GENERATED ALWAYS AS IDENTITY` syntax to
// an expression other than "DEFAULT".
func NewGeneratedAlwaysAsIdentityColumnUpdateError(columnName string) error {
	return errors.WithDetailf(
		pgerror.Newf(pgcode.GeneratedAlways, "column %q can only be updated to DEFAULT", columnName),
		"Column %q is an identity column defined as GENERATED ALWAYS", columnName,
	)
}

// NewIdentityColumnTypeError creates an error for declaring an IDENTITY column
// with a non-integer type.
func NewIdentityColumnTypeError() error {
	return pgerror.Newf(pgcode.InvalidParameterValue,
		"identity column type must be INT, INT2, INT4 or INT8")
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

// NewUndefinedObjectError creates an error that represents a missing object.
func NewUndefinedObjectError(name tree.NodeFormatter) error {
	return pgerror.Newf(pgcode.UndefinedObject, "object %q does not exist", tree.ErrString(name))
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

// NewColumnAlreadyExistsInRelationError creates an error for a preexisting column in relation.
func NewColumnAlreadyExistsInRelationError(name, relation string) error {
	return pgerror.Newf(pgcode.DuplicateColumn, "column %q of relation %q already exists", name, relation)
}

// NewColumnAlreadyExistsInIndexError creates an error for a  preexisting column in index.
func NewColumnAlreadyExistsInIndexError(idxName, colName string) error {
	return pgerror.Newf(pgcode.DuplicateColumn, "index %q already contains column %q", idxName, colName)
}

// NewDatabaseAlreadyExistsError creates an error for a preexisting database.
func NewDatabaseAlreadyExistsError(name string) error {
	return pgerror.Newf(pgcode.DuplicateDatabase, "database %q already exists", name)
}

// NewSchemaAlreadyExistsError creates an error for a preexisting schema.
func NewSchemaAlreadyExistsError(name string) error {
	return pgerror.Newf(pgcode.DuplicateSchema, "schema %q already exists", name)
}

func NewUnsupportedUnvalidatedConstraintError(constraintType catconstants.ConstraintType) error {
	return pgerror.Newf(pgcode.FeatureNotSupported,
		"%v constraints cannot be marked NOT VALID", constraintType)
}

// NewInvalidActionOnComputedFKColumnError creates an error when there is an
// attempt to have an unsupported action on a FK over a computed column.
func NewInvalidActionOnComputedFKColumnError(onUpdateAction bool) error {
	// Pick the 'ON UPDATE' or 'ON DELETE' keyword. If both actions are set we
	// include 'ON UPDATE' in the error text. This is consistent with postgres.
	var keyword redact.SafeString = "DELETE"
	if onUpdateAction {
		keyword = "UPDATE"
	}
	return pgerror.Newf(pgcode.InvalidForeignKey,
		"invalid ON %s action for foreign key constraint containing computed column", keyword,
	)
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

// NewDependentBlocksOpError creates an error because dependingObjName (of type
// dependingObjType) has a reference to objName (of objType) when someone attempts
// to `op` on it.
// E.g. DROP INDEX "idx" when a VIEW "v" depends on this index and thus will block
// this drop index.
func NewDependentBlocksOpError(op, objType, objName, dependentType, dependentName string) error {
	return errors.WithHintf(
		NewDependentObjectErrorf("cannot %s %s %q because %s %q depends on it",
			op, objType, objName, dependentType, dependentName),
		"consider dropping %q first.", dependentName)
}

func NewAlterColTypeInCombinationNotSupportedError() error {
	return unimplemented.NewWithIssuef(
		49351, "ALTER COLUMN TYPE cannot be used in combination "+
			"with other ALTER TABLE commands")
}

const PrimaryIndexSwapDetail = `CRDB's implementation for "ADD COLUMN", "DROP COLUMN", and "ALTER PRIMARY KEY" will drop the old/current primary index and create a new one.`

// NewColumnReferencedByPrimaryKeyError is returned when attempting to drop a
// column which is a part of the table's primary key.
//
// Note that this limitation is not fundamental; in postgres when dropping a
// primary key column, it would silently drop the primary key constraint. At
// the time of writing, cockroach does not permit dropping a primary key
// constraint as it may require populating a new implicit rowid column for the
// implicit primary key we use.
func NewColumnReferencedByPrimaryKeyError(colName string) error {
	return pgerror.Newf(pgcode.InvalidColumnReference,
		"column %q is referenced by the primary key", colName)
}

// NewAlterDependsOnDurationExprError creates an error for attempting to alter
// the internal column created for the duration expression. These are tables
// that define the ttl_expire_after setting.
func NewAlterDependsOnDurationExprError(op, objType, colName, tabName string) error {
	return errors.WithHintf(
		pgerror.Newf(
			pgcode.InvalidTableDefinition,
			`cannot %s %s %s while ttl_expire_after is set`,
			redact.SafeString(op), redact.SafeString(objType), colName,
		),
		"use ALTER TABLE %s RESET (ttl) instead",
		tabName,
	)
}

// NewAlterDependsOnExpirationExprError creates an error for attempting to alter
// the column that is referenced in the expiration expression.
func NewAlterDependsOnExpirationExprError(
	op, objType, colName, tabName, expirationExpr string,
) error {
	return errors.WithHintf(
		pgerror.Newf(
			pgcode.InvalidTableDefinition,
			`cannot %s %s %q referenced by row-level TTL expiration expression %q`,
			redact.SafeString(op), redact.SafeString(objType), colName,
			expirationExpr,
		),
		"use ALTER TABLE %s SET (ttl_expiration_expression = ...) to change the expression",
		tabName,
	)
}

// NewColumnReferencedByComputedColumnError is returned when dropping a column
// and that column being dropped is referenced by a computed column. Note that
// the cockroach behavior where this error is returned does not match the
// postgres behavior.
func NewColumnReferencedByComputedColumnError(droppingColumn, computedColumn string) error {
	return pgerror.Newf(
		pgcode.InvalidColumnReference,
		"column %q is referenced by computed column %q",
		droppingColumn,
		computedColumn,
	)
}

// ColumnReferencedByPartialIndex is returned when an attempt is made to
// modify a column that is referenced in a partial index's predicate.
func ColumnReferencedByPartialIndex(op, objType, column, partialIndex string) error {
	return errors.WithIssueLink(errors.WithHintf(
		pgerror.Newf(
			pgcode.InvalidColumnReference,
			"cannot %s %s %q because it is referenced by partial index %q",
			op, objType, column, partialIndex,
		),
		"drop the partial index first, then %s the %s",
		op, objType,
	), errors.IssueLink{IssueURL: build.MakeIssueURL(97372)})
}

// ColumnReferencedByPartialUniqueWithoutIndexConstraint is almost the same as
// ColumnReferencedByPartialIndex except it's used when altering a column that is
// referenced in a partial unique without index constraint's predicate.
func ColumnReferencedByPartialUniqueWithoutIndexConstraint(
	op, objType, column, partialUWIConstraint string,
) error {
	return errors.WithIssueLink(errors.WithHintf(
		pgerror.Newf(
			pgcode.InvalidColumnReference,
			"cannot %s %s %q because it is referenced by partial unique constraint %q",
			op, objType, column, partialUWIConstraint,
		),
		"drop the unique constraint first, then %s the %s",
		op, objType,
	), errors.IssueLink{IssueURL: build.MakeIssueURL(97372)})
}

// NewUniqueConstraintReferencedByForeignKeyError generates an error to be
// returned when dropping a unique constraint that is relied upon by an
// inbound foreign key constraint.
func NewUniqueConstraintReferencedByForeignKeyError(
	uniqueConstraintOrIndexToDrop, tableName string,
) error {
	return pgerror.Newf(
		pgcode.DependentObjectsStillExist,
		"%q is referenced by foreign key from table %q",
		uniqueConstraintOrIndexToDrop, tableName,
	)
}

// NewUndefinedUserError returns an undefined user error.
func NewUndefinedUserError(user username.SQLUsername) error {
	return pgerror.Newf(pgcode.UndefinedObject, "role/user %q does not exist", user)
}

// NewUndefinedConstraintError returns a missing constraint error.
func NewUndefinedConstraintError(constraintName, tableName string) error {
	return pgerror.Newf(pgcode.UndefinedObject,
		"constraint %q of relation %q does not exist", constraintName, tableName)
}

// NewUndefinedTriggerError returns a missing constraint error.
func NewUndefinedTriggerError(triggerName, tableName string) error {
	return pgerror.Newf(pgcode.UndefinedObject,
		"trigger %q of relation %q does not exist", triggerName, tableName)
}

// NewRangeUnavailableError creates an unavailable range error.
func NewRangeUnavailableError(rangeID roachpb.RangeID, origErr error) error {
	return pgerror.Wrapf(origErr, pgcode.RangeUnavailable, "key range id:%d is unavailable", rangeID)
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

// NewInvalidVolatilityError creates an error for the case when provided
// volatility options are not valid through CREATE/REPLACE/ALTER FUNCTION.
func NewInvalidVolatilityError(err error) error {
	return pgerror.Wrap(err, pgcode.InvalidFunctionDefinition, "invalid volatility")
}

func NewCannotModifyVirtualSchemaError(schema string) error {
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"%s is a virtual schema and cannot be modified", tree.ErrNameString(schema))
}

// NewInsufficientPrivilegeOnDescriptorError creates an InsufficientPrivilege
// error saying the `user` does not have any of the privilege in `orPrivs` on
// the descriptor.
func NewInsufficientPrivilegeOnDescriptorError(
	user username.SQLUsername, orPrivs []privilege.Kind, descType string, descName string,
) error {
	orPrivsInStr := make([]string, 0, len(orPrivs))
	for _, priv := range orPrivs {
		orPrivsInStr = append(orPrivsInStr, string(priv.DisplayName()))
	}
	sort.Strings(orPrivsInStr)
	privsStr := strings.Join(orPrivsInStr, " or ")
	return pgerror.Newf(pgcode.InsufficientPrivilege,
		"user %s does not have %s privilege on %s %s",
		user, privsStr, descType, descName)
}

// QueryTimeoutError is an error representing a query timeout.
var QueryTimeoutError = pgerror.New(
	pgcode.QueryCanceled, "query execution canceled due to statement timeout")

// TxnTimeoutError is an error representing a transasction timeout.
var TxnTimeoutError = pgerror.New(
	pgcode.QueryCanceled, "query execution canceled due to transaction timeout")

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

// IsUndefinedDatabaseError checks whether this is an undefined database error.
func IsUndefinedDatabaseError(err error) bool {
	return errHasCode(err, pgcode.UndefinedDatabase)
}

// IsUndefinedSchemaError checks whether this is an undefined schema error.
func IsUndefinedSchemaError(err error) bool {
	return errHasCode(err, pgcode.UndefinedSchema)
}

// IsMissingDescriptorError checks whether the error has any indication
// that it corresponds to a missing descriptor of any kind.
//
// Note that this does not deal with the lower-level
// catalog.ErrDescriptorNotFound error. That error should be transformed
// by this package for all uses in the SQL layer and coming out of
// descs.Collection functions.
func IsMissingDescriptorError(err error) bool {
	return IsUndefinedRelationError(err) ||
		IsUndefinedSchemaError(err) ||
		IsUndefinedDatabaseError(err) ||
		errHasCode(err, pgcode.UndefinedObject) ||
		errHasCode(err, pgcode.UndefinedFunction)
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

// IsDistSQLRetryableError returns true if the supplied error, or any of its parent
// causes is an rpc error.
// This is an unfortunate implementation that should be looking for a more
// specific error.
func IsDistSQLRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// TODO(knz): this is a bad implementation. Make it go away
	// by avoiding string comparisons.

	errStr := err.Error()
	// When a crdb node dies, any DistSQL flows with processors scheduled on
	// it get an error with "rpc error" in the message from the call to
	// `(*DistSQLPlanner).Run`.
	return strings.Contains(errStr, `rpc error`)
}

var (
	ErrEmptyDatabaseName = pgerror.New(pgcode.Syntax, "empty database name")
	ErrNoDatabase        = pgerror.New(pgcode.InvalidName, "no database specified")
	ErrNoSchema          = pgerror.Newf(pgcode.InvalidName, "no schema specified")
	ErrNoTable           = pgerror.New(pgcode.InvalidName, "no table specified")
	ErrNoType            = pgerror.New(pgcode.InvalidName, "no type specified")
	ErrNoFunction        = pgerror.New(pgcode.InvalidName, "no function specified")
	ErrNoMatch           = pgerror.New(pgcode.UndefinedObject, "no object matched")
)

var ErrNoZoneConfigApplies = errors.New("no zone config applies")
