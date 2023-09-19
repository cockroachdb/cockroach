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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

func offByDefaultCheck(mode sessiondatapb.NewSchemaChangerMode) bool {
	return mode == sessiondatapb.UseNewSchemaChangerUnsafeAlways ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafe
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
		return offByDefaultCheck(mode) && activeVersion.IsActive(clusterversion.V23_2)
	case *tree.CreateSequence:
		return offByDefaultCheck(mode) && activeVersion.IsActive(clusterversion.V23_2)
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
