// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

func alterTableRenameColumn(
	b BuildCtx,
	tn *tree.TableName,
	tbl *scpb.Table,
	n *tree.AlterTable,
	t *tree.AlterTableRenameColumn,
) {
	if len(n.Cmds) > 1 {
		panic(scerrors.NotImplementedError(n))
	}
	alterColumnPreChecks(b, tn, tbl, t.Column)

	// 1. Resolve the column by current name.
	eltsFromColName := b.ResolveColumn(tbl.TableID, t.Column, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	})
	colElt := eltsFromColName.FilterColumn().MustGetZeroOrOneElement()
	colNameElt := eltsFromColName.FilterColumnName().MustGetZeroOrOneElement()

	// 2. Validate column exists and the new name is valid..
	if colElt == nil {
		panic(pgerror.Newf(pgcode.UndefinedColumn, "column %q does not exist", tree.ErrString(&t.Column)))
	}
	if colNameElt == nil {
		panic(errors.AssertionFailedf("column element resolved for %s, but column name element not found", tree.ErrString(&t.Column)))
	}
	renameColumnChecks(b, colElt, t.Column, t.NewName)

	// Short circuit if the name is unchanged.
	if colNameElt.Name == string(t.NewName) {
		return
	}

	// Drop the old column name and add the new one to ensure proper rollback
	// behavior.
	b.Drop(colNameElt)
	b.Add(&scpb.ColumnName{
		TableID:  tbl.TableID,
		ColumnID: colElt.ColumnID,
		Name:     string(t.NewName),
	})
}

// renameColumnChecks validates that a column can be renamed and performs
// dependency checks. It ensures the new name is valid, verifies the column
// is not a system or shard column, checks for blocking dependencies from
// views and functions, and validates that inaccessible columns cannot be
// renamed.
func renameColumnChecks(b BuildCtx, col *scpb.Column, oldName, newName tree.Name) {
	if newName == "" {
		panic(pgerror.New(pgcode.Syntax, "empty column name"))
	}

	// Block renaming of system columns.
	panicIfSystemColumn(col, oldName)

	// Block renaming of shard columns.
	if isShardColumn(b, col) {
		panic(pgerror.Newf(pgcode.ReservedName, "cannot rename shard column"))
	}

	walkColumnDependencies(b, col, "rename", "column", func(e scpb.Element, op, objType string) {
		switch e := e.(type) {
		case *scpb.View:
			ns := b.QueryByID(col.TableID).FilterNamespace().MustGetOneElement()
			nsDep := b.QueryByID(e.ViewID).FilterNamespace().MustGetOneElement()
			if nsDep.DatabaseID != ns.DatabaseID || nsDep.SchemaID != ns.SchemaID {
				panic(sqlerrors.NewDependentBlocksOpError(op, objType, tree.ErrString(&oldName), "view", qualifiedName(b, e.ViewID)))
			}
			panic(sqlerrors.NewDependentBlocksOpError(op, objType, tree.ErrString(&oldName), "view", nsDep.Name))
		case *scpb.FunctionBody:
			_, _, fnName := scpb.FindFunctionName(b.QueryByID(e.FunctionID))
			panic(sqlerrors.NewDependentObjectErrorf(
				"cannot rename column %q because function %q depends on it",
				tree.ErrString(&oldName), fnName.Name),
			)
		}
	}, true /* allowPartialIdxPredicateRef */)

	if col.IsInaccessible {
		panic(pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot be renamed",
			tree.ErrString(&oldName),
		))
	}
	checkNewColumnNameConflicts(b, col.TableID, oldName, newName)
}

// checkNewColumnNameConflicts verifies that the new column name does not
// conflict with existing columns in the table. It checks for naming conflicts
// with system columns, columns currently being dropped, and columns being
// added in the same transaction.
func checkNewColumnNameConflicts(b BuildCtx, tableID catid.DescID, oldName, newName tree.Name) {
	b.QueryByID(tableID).FilterColumnName().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
		// If an existing column on our table has the same name, panic.
		if tree.Name(e.Name) == newName && tree.Name(e.Name) != oldName {
			existingColWithSameName := b.QueryByID(tableID).FilterColumn().Filter(
				func(_ scpb.Status, _ scpb.TargetStatus, ce *scpb.Column) bool {
					return ce.ColumnID == e.ColumnID
				}).MustGetOneElement()
			if existingColWithSameName.IsSystemColumn {
				panic(pgerror.Newf(pgcode.DuplicateColumn,
					"column name %q conflicts with a system column name",
					tree.ErrString(&newName)))
			}
			if current == scpb.Status_PUBLIC {
				if target == scpb.ToAbsent {
					panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "column %q being dropped, try again later", tree.ErrString(&newName)))
				}
				tableName := b.QueryByID(tableID).FilterNamespace().MustGetOneElement()
				panic(sqlerrors.NewColumnAlreadyExistsInRelationError(tree.ErrString(&newName), tableName.Name))
			}
			if current == scpb.Status_ABSENT && target == scpb.ToPublic {
				panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "column %q being dropped, try again later", tree.ErrString(&newName)))
			}
		}
	})
}
