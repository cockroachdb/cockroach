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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

func offByDefaultCheck(mode sessiondatapb.NewSchemaChangerMode) bool {
	return mode == sessiondatapb.UseNewSchemaChangerUnsafeAlways ||
		mode == sessiondatapb.UseNewSchemaChangerUnsafe
}

type ProcessFunc func(BuildCtx)

func (pf ProcessFunc) IsSupported() bool {
	return pf != nil
}

// Process dispatches on the statement type to populate the BuilderState
// embedded in the BuildCtx. Any error will be panicked.
func Process(
	n tree.Statement,
	activeVersion clusterversion.ClusterVersion,
	mode sessiondatapb.NewSchemaChangerMode,
) ProcessFunc {
	var pf ProcessFunc
	switch typedN := n.(type) {
	case *tree.AlterTable:
		pf = AlterTable(typedN, activeVersion)
	case *tree.CreateIndex:
		if activeVersion.IsActive(clusterversion.V23_1) {
			pf = func(b BuildCtx) {
				CreateIndex(b, typedN)
			}
		}
	case *tree.DropDatabase:
		pf = func(b BuildCtx) {
			DropDatabase(b, typedN)
		}
	case *tree.DropOwnedBy:
		pf = func(b BuildCtx) {
			DropOwnedBy(b, typedN)
		}
	case *tree.DropSchema:
		pf = func(b BuildCtx) {
			DropSchema(b, typedN)
		}
	case *tree.DropSequence:
		pf = func(b BuildCtx) {
			DropSequence(b, typedN)
		}
	case *tree.DropTable:
		pf = func(b BuildCtx) {
			DropTable(b, typedN)
		}
	case *tree.DropType:
		pf = func(b BuildCtx) {
			DropType(b, typedN)
		}
	case *tree.DropView:
		pf = func(b BuildCtx) {
			DropView(b, typedN)
		}
	case *tree.CommentOnConstraint:
		pf = func(b BuildCtx) {
			CommentOnConstraint(b, typedN)
		}
	case *tree.CommentOnDatabase:
		pf = func(b BuildCtx) {
			CommentOnDatabase(b, typedN)
		}
	case *tree.CommentOnSchema:
		pf = func(b BuildCtx) {
			CommentOnSchema(b, typedN)
		}
	case *tree.CommentOnTable:
		pf = func(b BuildCtx) {
			CommentOnTable(b, typedN)
		}
	case *tree.CommentOnColumn:
		pf = func(b BuildCtx) {
			CommentOnColumn(b, typedN)
		}
	case *tree.CommentOnIndex:
		pf = func(b BuildCtx) {
			CommentOnIndex(b, typedN)
		}
	case *tree.DropIndex:
		if activeVersion.IsActive(clusterversion.V23_1) {
			pf = func(b BuildCtx) {
				DropIndex(b, typedN)
			}
		}
	case *tree.DropFunction:
		if activeVersion.IsActive(clusterversion.V23_1) {
			pf = func(b BuildCtx) {
				DropFunction(b, typedN)
			}
		}
	case *tree.CreateRoutine:
		if activeVersion.IsActive(clusterversion.V23_1) {
			pf = func(b BuildCtx) {
				CreateFunction(b, typedN)
			}
		}
	case *tree.CreateSchema:
		if offByDefaultCheck(mode) && activeVersion.IsActive(clusterversion.V23_2) {
			pf = func(b BuildCtx) {
				CreateSchema(b, typedN)
			}
		}
	case *tree.CreateSequence:
		if offByDefaultCheck(mode) && activeVersion.IsActive(clusterversion.V23_2) {
			pf = func(b BuildCtx) {
				CreateSequence(b, typedN)
			}
		}
	}
	if !pf.IsSupported() {
		return nil
	}
	// Check prereqs.
	return func(b BuildCtx) {
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
