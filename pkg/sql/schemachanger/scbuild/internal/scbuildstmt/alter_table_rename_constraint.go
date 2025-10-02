// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
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
	if len(n.Cmds) > 1 {
		panic(scerrors.NotImplementedError(n))
	}

	// 1. Resolve the constraint by current name.
	constraintElems := b.ResolveConstraint(tbl.TableID, t.Constraint, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	})

	// 2. Validate constraint exists.
	if constraintElems == nil {
		panic(sqlerrors.NewUndefinedConstraintError(tree.ErrString(&t.Constraint), tn.Table()))
	}

	// Short circuit if the name is unchanged.
	if t.Constraint == t.NewName {
		return
	}

	// 3. Handle both index-backed and non-index constraints.
	_, _, indexName := scpb.FindIndexName(constraintElems)
	_, _, constraintWithoutIndexName := scpb.FindConstraintWithoutIndexName(constraintElems)

	if indexName == nil && constraintWithoutIndexName == nil {
		panic(pgerror.Newf(pgcode.UndefinedObject, "constraint %q does not exist", t.Constraint))
	}

	// 4. Check for validating/dropping state.
	if constraintWithoutIndexName != nil {
		checkConstraintNotBeingModified(b, constraintElems, t.Constraint)
	}

	// 5. Check for name conflicts.
	checkConstraintNameConflict(b, tbl.TableID, t.Constraint, t.NewName)

	// 6. For index-backed constraints, also check for index name conflicts.
	if indexName != nil {
		checkIndexNameConflict(b, tbl.TableID, t.NewName)
		checkForDependentViews(b, tbl.TableID, indexName.IndexID, t.Constraint)
	}

	// 7. Rename based on constraint type.
	if indexName != nil {
		// Index-backed constraint (PRIMARY KEY, UNIQUE with index).
		b.Drop(indexName)
		b.Add(&scpb.IndexName{
			TableID: tbl.TableID,
			IndexID: indexName.IndexID,
			Name:    string(t.NewName),
		})

		fmt.Printf("DEBUG dropping index name %v and adding new index name %v\n", indexName.Name, t.NewName)
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

// checkConstraintNotBeingModified verifies that the constraint is not in the
// middle of being added or dropped.
func checkConstraintNotBeingModified(
	b BuildCtx, constraintElems ElementResultSet, constraintName tree.Name,
) {
	// Check if any constraint elements are in a non-public state moving to public
	// (being added/validated) or public moving to absent (being dropped).
	constraintElems.ForEach(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if current != scpb.Status_PUBLIC && target == scpb.ToPublic {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"constraint %q in the middle of being added, try again later", constraintName))
		}
		if current == scpb.Status_PUBLIC && target == scpb.ToAbsent {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"constraint %q in the middle of being dropped", constraintName))
		}
	})
}

// checkConstraintNameConflict verifies that the new constraint name does not
// conflict with existing constraints in the table.
func checkConstraintNameConflict(b BuildCtx, tableID catid.DescID, oldName, newName tree.Name) {
	// Try to resolve a constraint with the new name.
	existingConstraint := b.ResolveConstraint(tableID, newName, ResolveParams{
		IsExistenceOptional: true,
		RequiredPrivilege:   privilege.USAGE,
	})

	// If a constraint with the new name exists and it's not the one we're renaming, panic.
	if existingConstraint != nil {
		panic(pgerror.Newf(pgcode.DuplicateObject,
			"duplicate constraint name: %q", newName))
	}
}

// checkIndexNameConflict verifies that no index with the new name exists.
// This is necessary for index-backed constraints where renaming the constraint
// also renames the underlying index.
func checkIndexNameConflict(b BuildCtx, tableID catid.DescID, newName tree.Name) {
	scpb.ForEachIndexName(b.QueryByID(tableID), func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.IndexName,
	) {
		if e.Name == string(newName) && target == scpb.ToPublic {
			panic(pgerror.Newf(pgcode.DuplicateRelation,
				"relation %v already exists", newName))
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
