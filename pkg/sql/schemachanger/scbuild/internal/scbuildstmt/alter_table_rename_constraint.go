// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

func alterTableRenameConstraint(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	n *tree.AlterTable,
	t *tree.AlterTableRenameConstraint,
) {
	// Resolve the constraint by current name.
	constraintElems := b.ResolveConstraint(tbl.TableID, t.Constraint, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	})

	// Validate constraint exists.
	if constraintElems == nil {
		panic(sqlerrors.NewUndefinedConstraintError(tree.ErrString(&t.Constraint), tn.Table()))
	}

	// Short circuit if the name is unchanged.
	if t.Constraint == t.NewName {
		return
	}

	// Handle both index-backed and non-index constraints.
	indexName := constraintElems.FilterIndexName().NotToAbsent().MustGetZeroOrOneElement()
	constraintWithoutIndexName := constraintElems.FilterConstraintWithoutIndexName().NotToAbsent().MustGetZeroOrOneElement()

	if indexName == nil && constraintWithoutIndexName == nil {
		panic(pgerror.Newf(pgcode.UndefinedObject, "constraint %q does not exist", t.Constraint))
	}

	// Check for name conflicts across all constraint types.
	// This must check both ConstraintWithoutIndexName (CHECK, FK, UNIQUE without
	// index) and IndexName (UNIQUE with index, PRIMARY KEY) to prevent a CHECK
	// constraint from being renamed to match a UNIQUE constraint name.
	checkConstraintNameConflicts(b, tbl.TableID, t.NewName)

	// For index-backed constraints, check for dependent views.
	if indexName != nil {
		checkForDependentViews(b, tbl.TableID, indexName.IndexID, t.Constraint)
	}

	// Rename based on constraint type.
	if indexName != nil {
		// Index-backed constraint (PRIMARY KEY, UNIQUE with index).
		b.Drop(indexName)
		b.Add(&scpb.IndexName{
			TableID: tbl.TableID,
			IndexID: indexName.IndexID,
			Name:    string(t.NewName),
		})
	} else {
		// Non-index constraint (CHECK, FK, UNIQUE without index).
		b.Drop(constraintWithoutIndexName)
		b.Add(&scpb.ConstraintWithoutIndexName{
			TableID:      tbl.TableID,
			ConstraintID: constraintWithoutIndexName.ConstraintID,
			Name:         string(t.NewName),
		})
	}
}

// checkConstraintNameConflicts verifies that the new constraint name does
// not conflict with any existing constraint in the table, across all constraint
// types. This checks both ConstraintWithoutIndexName elements (CHECK, FK, UNIQUE
// without index) and IndexName elements (UNIQUE with index, PRIMARY KEY).
// This comprehensive check is necessary because constraint names must be unique
// across all types - a CHECK constraint cannot have the same name as a UNIQUE
// constraint.
func checkConstraintNameConflicts(b BuildCtx, tableID catid.DescID, newName tree.Name) {
	// Check ConstraintWithoutIndexName elements (CHECK, FK, UNIQUE without index).
	tableElems := b.QueryByID(tableID)
	conflictingConstraints := tableElems.FilterConstraintWithoutIndexName().Filter(func(
		_ scpb.Status, target scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName,
	) bool {
		return tree.Name(e.Name) == newName && target == scpb.ToPublic
	})

	if !conflictingConstraints.IsEmpty() {
		panic(pgerror.Newf(pgcode.DuplicateObject, "duplicate constraint name: %q", newName))
	}

	// Check IndexName elements. These could be either index-backed constraints
	// (UNIQUE with index, PRIMARY KEY) or regular indexes.
	conflictingIndexNames := tableElems.FilterIndexName().Filter(func(
		_ scpb.Status, target scpb.TargetStatus, e *scpb.IndexName,
	) bool {
		return e.Name == string(newName) && target == scpb.ToPublic
	})

	conflictingIndexNames.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, indexNameElem *scpb.IndexName) {
		// Check if this index backs a constraint by looking for corresponding
		// index elements with ConstraintID set.
		secondaryIndex := tableElems.FilterSecondaryIndex().Filter(func(
			_ scpb.Status, _ scpb.TargetStatus, idx *scpb.SecondaryIndex,
		) bool {
			return idx.IndexID == indexNameElem.IndexID && idx.ConstraintID != 0
		})

		primaryIndex := tableElems.FilterPrimaryIndex().Filter(func(
			_ scpb.Status, _ scpb.TargetStatus, idx *scpb.PrimaryIndex,
		) bool {
			return idx.IndexID == indexNameElem.IndexID
		})

		isConstraintIndex := !secondaryIndex.IsEmpty() || !primaryIndex.IsEmpty()

		if isConstraintIndex {
			panic(pgerror.Newf(pgcode.DuplicateObject, "duplicate constraint name: %q", newName))
		} else {
			panic(pgerror.Newf(pgcode.DuplicateRelation, "relation %v already exists", newName))
		}
	})
}

// checkForDependentViews ensures that views don't depend on the index being
// renamed. This follows the same logic as the legacy schema changer.
func checkForDependentViews(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID, constraintName tree.Name,
) {
	// Check for views that depend on this index.
	scpb.ForEachView(b.BackReferences(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.View,
	) {
		// Check if the view depends on this specific index.
		for _, forwardRef := range e.ForwardReferences {
			if forwardRef.IndexID != indexID {
				continue
			}

			// This view depends on the index-backed constraint being renamed.
			viewName := b.QueryByID(e.ViewID).FilterNamespace().MustGetOneElement()
			tableName := b.QueryByID(tableID).FilterNamespace().MustGetOneElement()
			// Check if view is in the same database and schema.
			if viewName.DatabaseID != tableName.DatabaseID || viewName.SchemaID != tableName.SchemaID {
				panic(sqlerrors.NewDependentBlocksOpError("rename", "constraint",
					tree.ErrString(&constraintName), "view", qualifiedName(b, e.ViewID)))
			}
			panic(sqlerrors.NewDependentBlocksOpError("rename", "constraint",
				tree.ErrString(&constraintName), "view", viewName.Name))
		}
	})
}
