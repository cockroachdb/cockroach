// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/errors"
)

// supportedStatement tracks metadata for statements that are
// implemented by the new schema changer.
type supportedStatement struct {
	// fn is a function to perform a schema change.
	fn interface{}
	// statementTag tag for this statement.
	statementTag string
	// checks contains a coarse-grained function to filter out most
	// unsupported statements.
	// It's possible for certain unsupported statements to pass it but will
	// eventually be discovered in the builder and cause an unimplemented panic.
	// It should only contain "simple" checks that does not require us to resolve
	// the descriptor and its elements, so we can avoid unnecessary round-trips
	// made in the builder.
	checks interface{}
	// on indicates that this statement is on by default.
	on bool
}

// Tracks operations which are fully supported when the declarative schema
// changer is enabled. Operations marked as non-fully supported can only be
// with the use_declarative_schema_changer session variable.
var supportedStatements = map[reflect.Type]supportedStatement{
	// Alter table will have commands individually whitelisted via the
	// supportedAlterTableStatements list, so wwe will consider it fully supported
	// here.
	reflect.TypeOf((*tree.AlterTable)(nil)):          {fn: AlterTable, statementTag: tree.AlterTableTag, on: true, checks: alterTableChecks},
	reflect.TypeOf((*tree.CreateIndex)(nil)):         {fn: CreateIndex, statementTag: tree.CreateIndexTag, on: true, checks: isV231Active},
	reflect.TypeOf((*tree.DropDatabase)(nil)):        {fn: DropDatabase, statementTag: tree.DropDatabaseTag, on: true, checks: isV221Active},
	reflect.TypeOf((*tree.DropOwnedBy)(nil)):         {fn: DropOwnedBy, statementTag: tree.DropOwnedByTag, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.DropSchema)(nil)):          {fn: DropSchema, statementTag: tree.DropSchemaTag, on: true, checks: isV221Active},
	reflect.TypeOf((*tree.DropSequence)(nil)):        {fn: DropSequence, statementTag: tree.DropSequenceTag, on: true, checks: isV221Active},
	reflect.TypeOf((*tree.DropTable)(nil)):           {fn: DropTable, statementTag: tree.DropTableTag, on: true, checks: isV221Active},
	reflect.TypeOf((*tree.DropType)(nil)):            {fn: DropType, statementTag: tree.DropTypeTag, on: true, checks: isV221Active},
	reflect.TypeOf((*tree.DropView)(nil)):            {fn: DropView, statementTag: tree.DropViewTag, on: true, checks: isV221Active},
	reflect.TypeOf((*tree.CommentOnConstraint)(nil)): {fn: CommentOnConstraint, statementTag: tree.CommentOnConstraintTag, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.CommentOnDatabase)(nil)):   {fn: CommentOnDatabase, statementTag: tree.CommentOnDatabaseTag, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.CommentOnSchema)(nil)):     {fn: CommentOnSchema, statementTag: tree.CommentOnSchemaTag, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.CommentOnTable)(nil)):      {fn: CommentOnTable, statementTag: tree.CommentOnTableTag, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.CommentOnColumn)(nil)):     {fn: CommentOnColumn, statementTag: tree.CommentOnColumnTag, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.CommentOnIndex)(nil)):      {fn: CommentOnIndex, statementTag: tree.CommentOnIndexTag, on: true, checks: isV222Active},
	reflect.TypeOf((*tree.DropIndex)(nil)):           {fn: DropIndex, statementTag: tree.DropIndexTag, on: true, checks: isV231Active},
	reflect.TypeOf((*tree.DropFunction)(nil)):        {fn: DropFunction, statementTag: tree.DropFunctionTag, on: true, checks: isV231Active},
	reflect.TypeOf((*tree.CreateRoutine)(nil)):       {fn: CreateFunction, statementTag: tree.CreateRoutineTag, on: true, checks: isV231Active},
	reflect.TypeOf((*tree.CreateSchema)(nil)):        {fn: CreateSchema, statementTag: tree.CreateSchemaTag, on: false, checks: isV232Active},
	reflect.TypeOf((*tree.CreateSequence)(nil)):      {fn: CreateSequence, statementTag: tree.CreateSequenceTag, on: false, checks: isV232Active},
}

// supportedStatementTags tracks statement tags which are implemented
// by the declarative schema changer.
var supportedStatementTags = map[string]struct{}{}

func init() {
	boolType := reflect.TypeOf((*bool)(nil)).Elem()
	// Check function signatures inside the supportedStatements map.
	for statementType, statementEntry := range supportedStatements {
		// Validate main callback functions.
		callBackType := reflect.TypeOf(statementEntry.fn)
		if callBackType.Kind() != reflect.Func {
			panic(errors.AssertionFailedf("%v entry for statement is "+
				"not a function", statementType))
		}
		if callBackType.NumIn() != 2 ||
			!callBackType.In(0).Implements(reflect.TypeOf((*BuildCtx)(nil)).Elem()) ||
			callBackType.In(1) != statementType {
			panic(errors.AssertionFailedf("%v entry for statement "+
				"does not have a valid signature; got %v", statementType, callBackType))
		}
		// Validate fully supported function callbacks.
		if statementEntry.checks != nil {
			checks := reflect.TypeOf(statementEntry.checks)
			if checks.Kind() != reflect.Func {
				panic(errors.AssertionFailedf("%v entry for statement is "+
					"not a function", statementType))
			}
			if checks.NumIn() != 3 ||
				(checks.In(0) != statementType && !statementType.Implements(checks.In(0))) ||
				checks.In(1) != reflect.TypeOf(sessiondatapb.UseNewSchemaChangerOff) ||
				checks.In(2) != reflect.TypeOf((*clusterversion.ClusterVersion)(nil)).Elem() ||
				checks.NumOut() != 1 ||
				checks.Out(0) != boolType {
				panic(errors.AssertionFailedf("%v checks does not have a valid signature; got %v",
					statementType, checks))
			}
		}
		// Fetch the statement tag using the statement tag method on the type,
		// we can use this as a blacklist of blocked schema changes.
		supportedStatementTags[statementEntry.statementTag] = struct{}{}
	}
}

// IsFullySupportedWithFalsePositive returns if this statement is
// "fully supported" in the declarative schema changer under mode and active
// cluster version.
// It can return false positive but never false negative, because we only run
// a few "simple" checks that we cannot totally eliminate unsupported stmts;
// we will discover those in the builder (when we resolve descriptors and its
// elements) and panic with an unimplemented error.
func IsFullySupportedWithFalsePositive(
	n tree.Statement,
	activeVersion clusterversion.ClusterVersion,
	mode sessiondatapb.NewSchemaChangerMode,
) (ret bool) {
	return isFullySupportedWithFalsePositiveInternal(supportedStatements, reflect.TypeOf(n),
		reflect.ValueOf(n), mode, activeVersion)
}

// isFullySupportedWithFalsePositiveInternal determines whether a stmt is
// fully supported, with false positive, in the declarative schema changer,
// given mode the active cluster version.
func isFullySupportedWithFalsePositiveInternal(
	stmtSupportMap map[reflect.Type]supportedStatement,
	stmtType reflect.Type,
	stmtValue reflect.Value,
	mode sessiondatapb.NewSchemaChangerMode,
	activeVersion clusterversion.ClusterVersion,
) bool {
	if mode == sessiondatapb.UseNewSchemaChangerOff {
		return false
	}

	info, ok := stmtSupportMap[stmtType]
	if !ok {
		return false
	}

	// If extraFn does not pass, then it's not supported, regardless of mode.
	if info.checks != nil {
		fn := reflect.ValueOf(info.checks)
		in := []reflect.Value{stmtValue, reflect.ValueOf(mode), reflect.ValueOf(activeVersion)}
		values := fn.Call(in)
		if !values[0].Bool() {
			return false
		}
	}

	// We now can just return whether the statement is labelled as "on" or not,
	// with the possibility of mode forcing "not on" to be on.
	return info.on ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafeAlways ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafe
}

// Process dispatches on the statement type to populate the BuilderState
// embedded in the BuildCtx. Any error will be panicked.
func Process(b BuildCtx, n tree.Statement) {
	newSchemaChangerMode := getDeclarativeSchemaChangerModeForStmt(b, n)
	// Run a few "quick checks" to see if the statement is not supported.
	if !IsFullySupportedWithFalsePositive(n, b.EvalCtx().Settings.Version.ActiveVersion(b),
		newSchemaChangerMode) {
		panic(scerrors.NotImplementedError(n))
	}

	// Invoke the callback function, with the concrete types.
	info := supportedStatements[reflect.TypeOf(n)]
	fn := reflect.ValueOf(info.fn)
	in := []reflect.Value{reflect.ValueOf(b), reflect.ValueOf(n)}
	// Check if the feature flag for it is enabled.
	err := b.CheckFeature(b, tree.GetSchemaFeatureNameFromStmt(n))
	if err != nil {
		panic(err)
	}
	fn.Call(in)
}

// getDeclarativeSchemaChangerModeForStmt returns the mode specific for `n`.
// It almost always returns value of session variable
// `use_declarative_schema_changer`, unless `n` is forcefully enabled (or
// disabled) via cluster setting `sql.schema.force_declarative_statements`, in
// which case it returns `unsafe` (or `off`).
func getDeclarativeSchemaChangerModeForStmt(
	b BuildCtx, n tree.Statement,
) sessiondatapb.NewSchemaChangerMode {
	ret := b.EvalCtx().SessionData().NewSchemaChangerMode
	// Check if the feature is either forcefully enabled or disabled, via a
	// cluster setting.
	stmtsForceControl := getStatementsForceControl(&b.ClusterSettings().SV)
	if forcedEnabled := stmtsForceControl.CheckControl(n); forcedEnabled {
		ret = sessiondatapb.UseNewSchemaChangerUnsafe
	}
	return ret
}

var isV221Active = func(_ tree.NodeFormatter, _ sessiondatapb.NewSchemaChangerMode, activeVersion clusterversion.ClusterVersion) bool {
	return activeVersion.IsActive(clusterversion.TODODelete_V22_1)
}

var isV222Active = func(_ tree.NodeFormatter, _ sessiondatapb.NewSchemaChangerMode, activeVersion clusterversion.ClusterVersion) bool {
	return activeVersion.IsActive(clusterversion.V22_2)
}

var isV231Active = func(_ tree.NodeFormatter, _ sessiondatapb.NewSchemaChangerMode, activeVersion clusterversion.ClusterVersion) bool {
	return activeVersion.IsActive(clusterversion.V23_1)
}

var isV232Active = func(_ tree.NodeFormatter, _ sessiondatapb.NewSchemaChangerMode, activeVersion clusterversion.ClusterVersion) bool {
	return activeVersion.IsActive(clusterversion.V23_2)
}
