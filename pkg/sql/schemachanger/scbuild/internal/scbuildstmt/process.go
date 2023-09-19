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
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

// supportedStatement tracks metadata for statements that are
// implemented by the new schema changer.
type supportedStatement struct {
	// statementTag tag for this statement.
	statementTag string
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
	reflect.TypeOf((*tree.AlterTable)(nil)):          {statementTag: tree.AlterTableTag, on: true},
	reflect.TypeOf((*tree.CreateIndex)(nil)):         {statementTag: tree.CreateIndexTag, on: true},
	reflect.TypeOf((*tree.DropDatabase)(nil)):        {statementTag: tree.DropDatabaseTag, on: true},
	reflect.TypeOf((*tree.DropOwnedBy)(nil)):         {statementTag: tree.DropOwnedByTag, on: true},
	reflect.TypeOf((*tree.DropSchema)(nil)):          {statementTag: tree.DropSchemaTag, on: true},
	reflect.TypeOf((*tree.DropSequence)(nil)):        {statementTag: tree.DropSequenceTag, on: true},
	reflect.TypeOf((*tree.DropTable)(nil)):           {statementTag: tree.DropTableTag, on: true},
	reflect.TypeOf((*tree.DropType)(nil)):            {statementTag: tree.DropTypeTag, on: true},
	reflect.TypeOf((*tree.DropView)(nil)):            {statementTag: tree.DropViewTag, on: true},
	reflect.TypeOf((*tree.CommentOnConstraint)(nil)): {statementTag: tree.CommentOnConstraintTag, on: true},
	reflect.TypeOf((*tree.CommentOnDatabase)(nil)):   {statementTag: tree.CommentOnDatabaseTag, on: true},
	reflect.TypeOf((*tree.CommentOnSchema)(nil)):     {statementTag: tree.CommentOnSchemaTag, on: true},
	reflect.TypeOf((*tree.CommentOnTable)(nil)):      {statementTag: tree.CommentOnTableTag, on: true},
	reflect.TypeOf((*tree.CommentOnColumn)(nil)):     {statementTag: tree.CommentOnColumnTag, on: true},
	reflect.TypeOf((*tree.CommentOnIndex)(nil)):      {statementTag: tree.CommentOnIndexTag, on: true},
	reflect.TypeOf((*tree.DropIndex)(nil)):           {statementTag: tree.DropIndexTag, on: true},
	reflect.TypeOf((*tree.DropFunction)(nil)):        {statementTag: tree.DropFunctionTag, on: true},
	reflect.TypeOf((*tree.CreateRoutine)(nil)):       {statementTag: tree.CreateRoutineTag, on: true},
	reflect.TypeOf((*tree.CreateSchema)(nil)):        {statementTag: tree.CreateSchemaTag, on: false},
	reflect.TypeOf((*tree.CreateSequence)(nil)):      {statementTag: tree.CreateSequenceTag, on: false},
}

// supportedStatementTags tracks statement tags which are implemented
// by the declarative schema changer.
var supportedStatementTags = map[string]struct{}{}

func init() {
	// Check function signatures inside the supportedStatements map.
	for _, statementEntry := range supportedStatements {
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
	if mode == sessiondatapb.UseNewSchemaChangerOff {
		return false
	}

	info, ok := supportedStatements[reflect.TypeOf(n)]
	if !ok {
		return false
	}
	if isOn := info.on ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafeAlways ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafe; !isOn {
		return false
	}

	switch typedN := n.(type) {
	case *tree.AlterTable:
		return alterTableChecks(typedN, mode, activeVersion)
	case *tree.CreateIndex:
		return activeVersion.IsActive(clusterversion.V23_1)
	case *tree.DropDatabase:
		return activeVersion.IsActive(clusterversion.TODODelete_V22_1)
	case *tree.DropOwnedBy:
		return activeVersion.IsActive(clusterversion.V22_2)
	case *tree.DropSchema:
		return activeVersion.IsActive(clusterversion.TODODelete_V22_1)
	case *tree.DropSequence:
		return activeVersion.IsActive(clusterversion.TODODelete_V22_1)
	case *tree.DropTable:
		return activeVersion.IsActive(clusterversion.TODODelete_V22_1)
	case *tree.DropType:
		return activeVersion.IsActive(clusterversion.TODODelete_V22_1)
	case *tree.DropView:
		return activeVersion.IsActive(clusterversion.TODODelete_V22_1)
	case *tree.CommentOnConstraint:
		return activeVersion.IsActive(clusterversion.V22_2)
	case *tree.CommentOnDatabase:
		return activeVersion.IsActive(clusterversion.V22_2)
	case *tree.CommentOnSchema:
		return activeVersion.IsActive(clusterversion.V22_2)
	case *tree.CommentOnTable:
		return activeVersion.IsActive(clusterversion.V22_2)
	case *tree.CommentOnColumn:
		return activeVersion.IsActive(clusterversion.V22_2)
	case *tree.CommentOnIndex:
		return activeVersion.IsActive(clusterversion.V22_2)
	case *tree.DropIndex:
		return activeVersion.IsActive(clusterversion.V23_1)
	case *tree.DropFunction:
		return activeVersion.IsActive(clusterversion.V23_1)
	case *tree.CreateRoutine:
		return activeVersion.IsActive(clusterversion.V23_1)
	case *tree.CreateSchema:
		return activeVersion.IsActive(clusterversion.V23_2)
	case *tree.CreateSequence:
		return activeVersion.IsActive(clusterversion.V23_2)
	default:
		return false
	}
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

	// Check if the feature flag for it is enabled.
	err := b.CheckFeature(b, tree.GetSchemaFeatureNameFromStmt(n))
	if err != nil {
		panic(err)
	}

	switch typedN := n.(type) {
	case *tree.AlterTable:
		AlterTable(b, typedN)
	case *tree.CreateIndex:
		CreateIndex(b, typedN)
	case *tree.DropDatabase:
		DropDatabase(b, typedN)
	case *tree.DropOwnedBy:
		DropOwnedBy(b, typedN)
	case *tree.DropSchema:
		DropSchema(b, typedN)
	case *tree.DropSequence:
		DropSequence(b, typedN)
	case *tree.DropTable:
		DropTable(b, typedN)
	case *tree.DropType:
		DropType(b, typedN)
	case *tree.DropView:
		DropView(b, typedN)
	case *tree.CommentOnConstraint:
		CommentOnConstraint(b, typedN)
	case *tree.CommentOnDatabase:
		CommentOnDatabase(b, typedN)
	case *tree.CommentOnSchema:
		CommentOnSchema(b, typedN)
	case *tree.CommentOnTable:
		CommentOnTable(b, typedN)
	case *tree.CommentOnColumn:
		CommentOnColumn(b, typedN)
	case *tree.CommentOnIndex:
		CommentOnIndex(b, typedN)
	case *tree.DropIndex:
		DropIndex(b, typedN)
	case *tree.DropFunction:
		DropFunction(b, typedN)
	case *tree.CreateRoutine:
		CreateFunction(b, typedN)
	case *tree.CreateSchema:
		CreateSchema(b, typedN)
	case *tree.CreateSequence:
		CreateSequence(b, typedN)
	default:
		panic(fmt.Sprintf("invalid statement %T", typedN))
	}
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
