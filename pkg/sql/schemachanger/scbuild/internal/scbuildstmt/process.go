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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

func offByDefaultCheck(mode sessiondatapb.NewSchemaChangerMode) bool {
	return mode == sessiondatapb.UseNewSchemaChangerUnsafeAlways ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafe
}

type ProcessFunc func(BuildCtx)

func wrapPF[T any](f func(b BuildCtx, t T), t T) ProcessFunc {
	return func(b BuildCtx) {
		f(b, t)
	}
}

// Process dispatches on the statement type to populate the BuilderState
// embedded in the BuildCtx. Any error will be panicked.
func Process(
	n tree.Statement,
	activeVersion clusterversion.ClusterVersion,
	mode sessiondatapb.NewSchemaChangerMode,
) (isSupported bool, _ ProcessFunc) {
	var pf ProcessFunc
	switch typedN := n.(type) {
	case *tree.AlterTable:
		pf = AlterTable(typedN, activeVersion)
	case *tree.CreateIndex:
		if activeVersion.IsActive(clusterversion.V23_1) {
			pf = wrapPF(CreateIndex, typedN)
		}
	case *tree.DropDatabase:
		pf = wrapPF(DropDatabase, typedN)
	case *tree.DropOwnedBy:
		pf = wrapPF(DropOwnedBy, typedN)
	case *tree.DropSchema:
		pf = wrapPF(DropSchema, typedN)
	case *tree.DropSequence:
		pf = wrapPF(DropSequence, typedN)
	case *tree.DropTable:
		pf = wrapPF(DropTable, typedN)
	case *tree.DropType:
		pf = wrapPF(DropType, typedN)
	case *tree.DropView:
		pf = wrapPF(DropView, typedN)
	case *tree.CommentOnConstraint:
		pf = wrapPF(CommentOnConstraint, typedN)
	case *tree.CommentOnDatabase:
		pf = wrapPF(CommentOnDatabase, typedN)
	case *tree.CommentOnSchema:
		pf = wrapPF(CommentOnSchema, typedN)
	case *tree.CommentOnTable:
		pf = wrapPF(CommentOnTable, typedN)
	case *tree.CommentOnColumn:
		pf = wrapPF(CommentOnColumn, typedN)
	case *tree.CommentOnIndex:
		pf = wrapPF(CommentOnIndex, typedN)
	case *tree.DropIndex:
		if activeVersion.IsActive(clusterversion.V23_1) {
			pf = wrapPF(DropIndex, typedN)
		}
	case *tree.DropFunction:
		if activeVersion.IsActive(clusterversion.V23_1) {
			pf = wrapPF(DropFunction, typedN)
		}
	case *tree.CreateRoutine:
		if activeVersion.IsActive(clusterversion.V23_1) {
			pf = wrapPF(CreateFunction, typedN)
		}
	case *tree.CreateSchema:
		if offByDefaultCheck(mode) && activeVersion.IsActive(clusterversion.V23_2) {
			pf = wrapPF(CreateSchema, typedN)
		}
	case *tree.CreateSequence:
		if offByDefaultCheck(mode) && activeVersion.IsActive(clusterversion.V23_2) {
			pf = wrapPF(CreateSequence, typedN)
		}
	}
	isSupported = pf != nil
	return isSupported, func(b BuildCtx) {
		if !isSupported {
			panic(scerrors.NotImplementedError(n))
		}
		// Check if the feature flag for it is enabled.
		err := b.CheckFeature(b, tree.GetSchemaFeatureNameFromStmt(n))
		if err != nil {
			panic(err)
		}
		pf(b)
	}
}

// GetDeclarativeSchemaChangerModeForStmt returns the mode specific for `n`.
// It almost always returns value of session variable
// `use_declarative_schema_changer`, unless `n` is forcefully enabled (or
// disabled) via cluster setting `sql.schema.force_declarative_statements`, in
// which case it returns `unsafe` (or `off`).
func GetDeclarativeSchemaChangerModeForStmt(
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
