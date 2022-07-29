// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rules

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

func idInIDs(objects []descpb.ID, id descpb.ID) bool {
	for _, other := range objects {
		if other == id {
			return true
		}
	}
	return false
}

func join(a, b nodeVars, attr rel.Attr, eqVarName rel.Var) rel.Clause {
	return joinOn(a, attr, b, attr, eqVarName)
}

var _ = join

func joinOn(a nodeVars, aAttr rel.Attr, b nodeVars, bAttr rel.Attr, eqVarName rel.Var) rel.Clause {
	return rel.And(
		a.el.AttrEqVar(aAttr, eqVarName),
		b.el.AttrEqVar(bAttr, eqVarName),
	)
}

func filterElements(name string, a, b nodeVars, fn interface{}) rel.Clause {
	return rel.Filter(name, a.el, b.el)(fn)
}

func toPublic(from, to nodeVars) rel.Clause {
	return toPublicUntyped(from.target, to.target)
}

func statusesToPublic(
	from nodeVars, fromStatus scpb.Status, to nodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		toPublic(from, to),
		from.currentStatus(fromStatus),
		to.currentStatus(toStatus),
	)
}

func toAbsent(from, to nodeVars) rel.Clause {
	return toAbsentUntyped(from.target, to.target)
}

func statusesToAbsent(
	from nodeVars, fromStatus scpb.Status, to nodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		toAbsent(from, to),
		from.currentStatus(fromStatus),
		to.currentStatus(toStatus),
	)
}

func joinOnDescID(a, b nodeVars, descriptorIDVar rel.Var) rel.Clause {
	return joinOnDescIDUntyped(a.el, b.el, descriptorIDVar)
}

func joinReferencedDescID(a, b nodeVars, descriptorIDVar rel.Var) rel.Clause {
	return joinReferencedDescIDUntyped(a.el, b.el, descriptorIDVar)
}

func joinOnColumnID(a, b nodeVars, relationIDVar, columnIDVar rel.Var) rel.Clause {
	return joinOnColumnIDUntyped(a.el, b.el, relationIDVar, columnIDVar)
}

func joinOnIndexID(a, b nodeVars, relationIDVar, indexIDVar rel.Var) rel.Clause {
	return joinOnIndexIDUntyped(a.el, b.el, relationIDVar, indexIDVar)
}

func joinOnConstraintID(a, b nodeVars, relationIDVar, constraintID rel.Var) rel.Clause {
	return joinOnConstraintIDUntyped(a.el, b.el, relationIDVar, constraintID)
}

func columnInIndex(
	indexColumn, index nodeVars, relationIDVar, columnIDVar, indexIDVar rel.Var,
) rel.Clause {
	return columnInIndexUntyped(indexColumn.el, index.el, relationIDVar, columnIDVar, indexIDVar)
}

func columnInPrimaryIndexSwap(
	indexColumn, index nodeVars, relationIDVar, columnIDVar, indexIDVar rel.Var,
) rel.Clause {
	return columnInPrimaryIndexSwapUntyped(indexColumn.el, index.el, relationIDVar, columnIDVar, indexIDVar)
}

var (
	toPublicUntyped = screl.Schema.Def2(
		"toPublic",
		"target1", "target2",
		func(target1 rel.Var, target2 rel.Var) rel.Clauses {
			return rel.Clauses{
				target1.AttrEq(screl.TargetStatus, scpb.Status_PUBLIC),
				target2.AttrEq(screl.TargetStatus, scpb.Status_PUBLIC),
			}
		})

	toAbsentUntyped = screl.Schema.Def2(
		"toAbsent",
		"target1", "target2",
		func(target1 rel.Var, target2 rel.Var) rel.Clauses {
			return rel.Clauses{
				target1.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
				target2.AttrEq(screl.TargetStatus, scpb.Status_ABSENT),
			}
		})

	joinReferencedDescIDUntyped = screl.Schema.Def3(
		"joinReferencedDescID", "referrer", "referenced", "id", func(
			referrer, referenced, id rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				referrer.AttrEqVar(screl.ReferencedDescID, id),
				referenced.AttrEqVar(screl.DescID, id),
			}
		})
	joinOnDescIDUntyped = screl.Schema.Def3(
		"joinOnDescID", "a", "b", "id", func(
			a, b, id rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				id.Entities(screl.DescID, a, b),
			}
		})
	joinOnIndexIDUntyped = screl.Schema.Def4(
		"joinOnIndexID", "a", "b", "desc-id", "index-id", func(
			a, b, descID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				joinOnDescIDUntyped(a, b, descID),
				indexID.Entities(screl.IndexID, a, b),
			}
		},
	)
	joinOnColumnIDUntyped = screl.Schema.Def4(
		"joinOnColumnID", "a", "b", "desc-id", "col-id", func(
			a, b, descID, colID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				joinOnDescIDUntyped(a, b, descID),
				colID.Entities(screl.ColumnID, a, b),
			}
		},
	)
	joinOnConstraintIDUntyped = screl.Schema.Def4(
		"joinOnConstraintID", "a", "b", "desc-id", "constraint-id", func(
			a, b, descID, constraintID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				joinOnDescIDUntyped(a, b, descID),
				constraintID.Entities(screl.ConstraintID, a, b),
			}
		},
	)

	columnInIndexUntyped = screl.Schema.Def5(
		"columnInIndex",
		"index-column", "index", "table-id", "column-id", "index-id", func(
			indexColumn, index, tableID, columnID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				indexColumn.Type((*scpb.IndexColumn)(nil)),
				indexColumn.AttrEqVar(screl.DescID, rel.Blank),
				indexColumn.AttrEqVar(screl.ColumnID, columnID),
				index.AttrEqVar(screl.IndexID, indexID),
				joinOnIndexIDUntyped(index, indexColumn, tableID, indexID),
			}
		})

	sourceIndexNotSetUntyped = screl.Schema.Def1("sourceIndexNotSet", "index", func(
		index rel.Var,
	) rel.Clauses {
		return rel.Clauses{
			index.AttrNeq(screl.SourceIndexID, catid.IndexID(0)),
		}
	})

	columnInPrimaryIndexSwapUntyped = screl.Schema.Def5(
		"columnInPrimaryIndexSwap",
		"index-column", "index", "table-id", "column-id", "index-id", func(
			indexColumn, index, tableID, columnID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				columnInIndexUntyped(
					indexColumn, index, tableID, columnID, indexID,
				),
				sourceIndexNotSetUntyped(index),
			}
		})
)

func forEachElement(fn func(element scpb.Element) error) error {
	var ep scpb.ElementProto
	vep := reflect.ValueOf(ep)
	for i := 0; i < vep.NumField(); i++ {
		e := vep.Field(i).Interface().(scpb.Element)
		if err := fn(e); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// IsDescriptor returns true for a descriptor-element, i.e. an element which
// owns its corresponding descriptor.
func IsDescriptor(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.Database, *scpb.Schema, *scpb.Table, *scpb.View, *scpb.Sequence, *scpb.AliasType, *scpb.EnumType:
		return true
	}
	return false
}

func isSubjectTo2VersionInvariant(e scpb.Element) bool {
	return isIndex(e) || isColumn(e)
}

func isIndex(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	}
	return false
}

func isColumn(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.Column:
		return true
	}
	return false
}

func isSimpleDependent(e scpb.Element) bool {
	return !IsDescriptor(e) && !isSubjectTo2VersionInvariant(e)
}

func getTypeT(element scpb.Element) (*scpb.TypeT, error) {
	switch e := element.(type) {
	case *scpb.ColumnType:
		if e == nil {
			return nil, nil
		}
		return &e.TypeT, nil
	case *scpb.AliasType:
		if e == nil {
			return nil, nil
		}
		return &e.TypeT, nil
	}
	return nil, errors.AssertionFailedf("element %T does not have an embedded scpb.TypeT", element)
}

func isWithTypeT(element scpb.Element) bool {
	_, err := getTypeT(element)
	return err == nil
}

func getTypeTOrPanic(element scpb.Element) *scpb.TypeT {
	ret, err := getTypeT(element)
	if err != nil {
		panic(err)
	}
	return ret
}

func getExpression(element scpb.Element) (*scpb.Expression, error) {
	switch e := element.(type) {
	case *scpb.ColumnType:
		if e == nil {
			return nil, nil
		}
		return e.ComputeExpr, nil
	case *scpb.ColumnDefaultExpression:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.ColumnOnUpdateExpression:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.SecondaryIndexPartial:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.CheckConstraint:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	}
	return nil, errors.AssertionFailedf("element %T does not have an embedded scpb.Expression", element)
}

func isWithExpression(element scpb.Element) bool {
	_, err := getExpression(element)
	return err == nil
}

func getExpressionOrPanic(element scpb.Element) *scpb.Expression {
	ret, err := getExpression(element)
	if err != nil {
		panic(err)
	}
	return ret
}

func isColumnDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ColumnType:
		return true
	case *scpb.ColumnName, *scpb.ColumnComment, *scpb.IndexColumn:
		return true
	}
	return isColumnTypeDependent(e)
}

func isColumnTypeDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.SequenceOwner, *scpb.ColumnDefaultExpression, *scpb.ColumnOnUpdateExpression:
		return true
	}
	return false
}

func isIndexDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.IndexName, *scpb.IndexComment, *scpb.IndexColumn:
		return true
	case *scpb.IndexPartitioning, *scpb.SecondaryIndexPartial:
		return true
	}
	return false
}
