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

	// 2. Validate column exists.
	if colElt == nil {
		panic(pgerror.Newf(pgcode.UndefinedColumn, "column %q does not exist", t.Column))
	}
	if colNameElt == nil {
		panic(errors.AssertionFailedf("column element resolved for %s, but column name element not found", t.Column))
	}

	if colNameElt.Name == t.NewName.String() {
		// Noop.
		return
	}

	renameColumnChecks(b, colElt, t.Column, t.NewName)

	b.Add(&scpb.ColumnName{
		TableID:  tbl.TableID,
		ColumnID: colElt.ColumnID,
		Name:     t.NewName.String(),
	})
}

// renameColumnChecks TODO
func renameColumnChecks(b BuildCtx, col *scpb.Column, oldName, newName tree.Name) {
	if newName == "" {
		panic(pgerror.New(pgcode.Syntax, "empty column name"))
	}

	// Block renaming of system columns.
	panicIfSystemColumn(col, oldName.String())
	walkColumnDependencies(b, col, "rename", "column", func(e scpb.Element, op, objType string) {
		switch e := e.(type) {
		case *scpb.View:
			ns := b.QueryByID(col.TableID).FilterNamespace().MustGetOneElement()
			nsDep := b.QueryByID(e.ViewID).FilterNamespace().MustGetOneElement()
			if nsDep.DatabaseID != ns.DatabaseID || nsDep.SchemaID != ns.SchemaID {
				panic(sqlerrors.NewDependentBlocksOpError(op, objType, oldName.String(), "view", qualifiedName(b, e.ViewID)))
			}
			panic(sqlerrors.NewDependentBlocksOpError(op, objType, oldName.String(), "view", nsDep.Name))
		case *scpb.Sequence:
			// something here
		}
	}, true)

	if col.IsInaccessible {
		panic(pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot be renamed",
			oldName.String(),
		))
	}
	checkNewColumnNameConflicts(b, col.TableID, newName)
}

// checkNewColumnNameConflicts TODO
func checkNewColumnNameConflicts(b BuildCtx, tableID catid.DescID, newName tree.Name) {
	b.QueryByID(tableID).FilterColumnName().ForEach(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
		// If an existing column on our table has the same name, panic.
		if e.Name == newName.String() {
			existingColWithSameName := b.QueryByID(tableID).FilterColumn().Filter(
				func(_ scpb.Status, _ scpb.TargetStatus, ce *scpb.Column) bool {
					return ce.ColumnID == e.ColumnID
				}).MustGetOneElement()
			if existingColWithSameName.IsSystemColumn {
				panic(pgerror.Newf(pgcode.DuplicateColumn,
					"column name %q conflicts with a system column name",
					newName.String()))
			}
			if current == scpb.Status_PUBLIC {
				if target == scpb.ToAbsent {
					panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "column %q being dropped, try again later", newName.String()))
				}
				tableName := b.QueryByID(tableID).FilterNamespace().MustGetOneElement()
				panic(sqlerrors.NewColumnAlreadyExistsInRelationError(tree.ErrString(&newName), tableName.Name))
			}
			if current == scpb.Status_ABSENT && target == scpb.ToPublic {
				panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "column %q being dropped, try again later", newName.String()))
			}
		}
	})

}
