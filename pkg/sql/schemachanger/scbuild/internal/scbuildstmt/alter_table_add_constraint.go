// Copyright 2022 The Cockroach Authors.
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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func alterTableAddConstraint(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddConstraint,
) {
	switch d := t.ConstraintDef.(type) {
	case *tree.UniqueConstraintTableDef:
		if d.PrimaryKey && t.ValidationBehavior == tree.ValidationDefault {
			alterTableAddPrimaryKey(b, tn, tbl, t)
		}
	case *tree.CheckConstraintTableDef:
		if t.ValidationBehavior == tree.ValidationDefault {
			alterTableAddCheck(b, tn, tbl, t)
		}
	}
}

// alterTableAddPrimaryKey contains logics for building ALTER TABLE ... ADD PRIMARY KEY.
// It assumes `t` is such a command.
func alterTableAddPrimaryKey(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddConstraint,
) {
	d := t.ConstraintDef.(*tree.UniqueConstraintTableDef)

	// Ensure that there is a default rowid column.
	oldPrimaryIndex := mustRetrievePrimaryIndexElement(b, tbl.TableID)
	if getPrimaryIndexDefaultRowIDColumn(
		b, tbl.TableID, oldPrimaryIndex.IndexID,
	) == nil {
		panic(scerrors.NotImplementedError(t))
	}
	alterPrimaryKey(b, tn, tbl, alterPrimaryKeySpec{
		n:             t,
		Columns:       d.Columns,
		Sharded:       d.Sharded,
		Name:          d.Name,
		StorageParams: d.StorageParams,
	})
}

// alterTableAddCheck contains logic for building ALTER TABLE ... ADD CONSTRAINT ... CHECK.
// It assumes `t` is such a command.
func alterTableAddCheck(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddConstraint,
) {
	// 1. Validate whether the to-be-created check constraint has a name that has already been used.
	if skip, err := validateConstraintNameIsNotUsed(b, tn, tbl, t); err != nil {
		panic(err)
	} else if skip {
		return
	}

	// 2. CheckDeepCopy whether this check constraint is syntactically valid. See the comments of
	// DequalifyAndValidateExprImpl for criteria.
	ckDef := t.ConstraintDef.(*tree.CheckConstraintTableDef)
	ckExpr, _, colIDs, err := schemaexpr.DequalifyAndValidateExprImpl(b, ckDef.Expr, types.Bool, "CHECK",
		b.SemaCtx(), volatility.Volatile, tn,
		func() colinfo.ResultColumns {
			return getNonDropResultColumns(b, tbl.TableID)
		},
		func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
			return columnLookupFn(b, tbl.TableID, columnName)
		},
	)
	if err != nil {
		panic(err)
	}
	typedCkExpr, err := parser.ParseExpr(ckExpr)
	if err != nil {
		panic(err)
	}

	// 3. Add relevant check constraint element: CheckConstraint and ConstraintName.
	constraintID := b.NextTableConstraintID(tbl.TableID)
	b.Add(&scpb.CheckConstraint{
		TableID:               tbl.TableID,
		ConstraintID:          constraintID,
		ColumnIDs:             colIDs.Ordered(),
		Expression:            *b.WrapExpression(tbl.TableID, typedCkExpr),
		FromHashShardedColumn: ckDef.FromHashShardedColumn,
		IndexIDForValidation:  getIndexIDForValidationForConstraint(b, tbl.TableID),
	})

	constraintName := string(ckDef.Name)
	if constraintName == "" {
		constraintName = generateUniqueCheckConstraintName(b, tbl.TableID, ckDef.Expr)
	}
	b.Add(&scpb.ConstraintWithoutIndexName{
		TableID:      tbl.TableID,
		ConstraintID: constraintID,
		Name:         constraintName,
	})
}

// getIndexIDForValidationForConstraint returns the index ID this check constraint is supposed to
// check against and will be used to hint the constraint validation query
// in `backfill.go`.
// Normally, it will return zero, which means we won't hint the validation query
// but instead let the optimizer pick the appropriate index to serve it.
// It will return non-zero if the constraint is added while a new column is being
// added (e.g. `ALTER TABLE t ADD COLUMN j INT DEFAULT 1 CHECK (j < 0);`), in which
// case we have to explicitly hint the validation query to check against the
// new primary index that is being added (as a result of adding a new column), instead
// of against the old primary index (which does not contain backfilled values for
// the new column and hence would mistakenly allow the validation query to succeed).
func getIndexIDForValidationForConstraint(b BuildCtx, tableID catid.DescID) (ret catid.IndexID) {
	b.QueryByID(tableID).ForEachElementStatus(func(current scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if pie, ok := e.(*scpb.PrimaryIndex); ok && target == scpb.ToPublic && current != scpb.Status_PUBLIC {
			ret = pie.IndexID
		}
	})
	return ret
}

// validateConstraintNameIsNotUsed checks that the name of the constraint we're
// trying to add isn't already used, and, if it is, whether the constraint
// addition should be skipped:
// - if the name is free to use, it returns false;
// - if it's already used but IF NOT EXISTS was specified, it returns true;
// - otherwise, it returns an error.
// TODO (xiang): It is written knowing that we will only see check constraint
// for now. Complete it by also considering all other types of constraints as well.
func validateConstraintNameIsNotUsed(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, t *tree.AlterTableAddConstraint,
) (skip bool, err error) {
	var name tree.Name
	var ifNotExists bool
	switch d := t.ConstraintDef.(type) {
	case *tree.CheckConstraintTableDef:
		name = d.Name
		ifNotExists = d.IfNotExists
	case *tree.ForeignKeyConstraintTableDef:
		panic(scerrors.NotImplementedErrorf(t, "FK constraint %v not yet implemented", d.Name))
	case *tree.UniqueConstraintTableDef:
		panic(scerrors.NotImplementedErrorf(t, "UNIQUE constraint %v not yet implemented", d.Name))
	default:
		return false, errors.AssertionFailedf(
			"unsupported constraint: %T", t.ConstraintDef)
	}

	if name == "" {
		// A name was not specified; We can give it any name later.
		return false, nil
	}

	var isInUse bool
	scpb.ForEachConstraintWithoutIndexName(b.QueryByID(tbl.TableID), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName,
	) {
		if e.Name == string(name) {
			isInUse = true
		}
	})

	if !isInUse {
		return false, nil
	}

	if ifNotExists {
		return true, nil
	}

	return false, pgerror.Newf(pgcode.DuplicateObject,
		"duplicate constraint name: %q", name)
}

// getNonDropResultColumns returns all public and adding columns, sorted by column ID in ascending order,
// in the format of ResultColumns.
func getNonDropResultColumns(b BuildCtx, tableID catid.DescID) (ret colinfo.ResultColumns) {
	for _, col := range getNonDropColumns(b, tableID) {
		ret = append(ret, colinfo.ResultColumn{
			Name:           mustRetrieveColumnNameElem(b, tableID, col.ColumnID).Name,
			Typ:            mustRetrieveColumnTypeElem(b, tableID, col.ColumnID).Type,
			Hidden:         col.IsHidden,
			TableID:        tableID,
			PGAttributeNum: uint32(col.PgAttributeNum),
		})
	}
	return ret
}

// columnLookupFn can look up information of a column by name.
// It should be exclusively used in DequalifyAndValidateExprImpl.
func columnLookupFn(
	b BuildCtx, tableID catid.DescID, columnName tree.Name,
) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
	columnID := getColumnIDFromColumnName(b, tableID, columnName)
	if columnID == 0 {
		return false, false, 0, nil
	}

	colElem := mustRetrieveColumnElem(b, tableID, columnID)
	colTypeElem := mustRetrieveColumnTypeElem(b, tableID, columnID)
	return true, !colElem.IsInaccessible, columnID, colTypeElem.Type
}

// generateUniqueCheckConstraintName generates a unique name for check constraint.
// The default name is `check_` followed by all columns in the expression (e.g. `check_a_b_a`).
// If that name is already in use, append a number (i.e. 1, 2, ...) to it until
// it's not in use.
func generateUniqueCheckConstraintName(b BuildCtx, tableID catid.DescID, ckExpr tree.Expr) string {
	name := checkConstraintDefaultName(b, tableID, ckExpr)

	if constraintNameInUse(b, tableID, name) {
		i := 1
		for {
			numberedName := fmt.Sprintf("%s%d", name, i)
			if !constraintNameInUse(b, tableID, numberedName) {
				name = numberedName
				break
			}
			i++
		}
	}

	return name
}

// constraintNameInUse returns whether `name` has been used by any other non-dropping constraint.
func constraintNameInUse(b BuildCtx, tableID catid.DescID, name string) (ret bool) {
	scpb.ForEachConstraintWithoutIndexName(b.QueryByID(tableID).Filter(publicTargetFilter), func(
		_ scpb.Status, _ scpb.TargetStatus, e *scpb.ConstraintWithoutIndexName,
	) {
		if e.Name == name {
			ret = true
		}
	})
	return ret
}

// DefaultName creates a check constraint name based on the columns referenced
// in the check expression. The format is "check_col1_col2...". If columns are
// duplicated in the expression, they will be duplicated in the name.
//
// For example:
//
//	CHECK (a < 0) => check_a
//	CHECK (a < 0 AND b = 'foo') => check_a_b
//	CHECK (a < 0 AND b = 'foo' AND a < 10) => check_a_b_a
//
// Note that the generated name is not guaranteed to be unique among the other
// constraints of the table.
func checkConstraintDefaultName(b BuildCtx, tableID catid.DescID, expr tree.Expr) string {
	var nameBuf bytes.Buffer
	nameBuf.WriteString("check")

	iterateColNamesInExpr(b, tableID, expr, func(columnName string) error {
		nameBuf.WriteByte('_')
		nameBuf.WriteString(columnName)
		return nil
	})

	return nameBuf.String()
}

// Iterate over all column names shown in an expression.
func iterateColNamesInExpr(
	b BuildCtx, tableID catid.DescID, rootExpr tree.Expr, f func(columnName string) error,
) {
	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			// Not a VarName, don't do anything to this node.
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		colElems := b.ResolveColumn(tableID, c.ColumnName, ResolveParams{
			IsExistenceOptional: false,
			RequiredPrivilege:   privilege.CREATE,
		})
		_, _, colNameElem := scpb.FindColumnName(colElems)
		if colNameElem == nil {
			panic(errors.AssertionFailedf("programming error: cannot find a Column element for column %v", c.ColumnName))
		}

		if err := f(colNameElem.Name); err != nil {
			return false, nil, err
		}
		return false, expr, err
	})

	if err != nil {
		panic(err)
	}
}
