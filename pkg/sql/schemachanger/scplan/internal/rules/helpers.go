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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

func depEdgeElementFilter(name string, a, b nodeVars, fn interface{}) rel.Clause {
	return rel.Filter(name, a.el, b.el)(fn)
}

func anyDepEdgeToPublic(from, to nodeVars) rel.Clause {
	return toPublicUntyped(from.target, to.target)
}

func depEdgeToPublic(
	from nodeVars, fromStatus scpb.Status, to nodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		anyDepEdgeToPublic(from, to),
		from.currentStatus(fromStatus),
		to.currentStatus(toStatus),
	)
}

func anyDepEdgeToAbsent(from, to nodeVars) rel.Clause {
	return toAbsentUntyped(from.target, to.target)
}

func depEdgeToAbsent(
	from nodeVars, fromStatus scpb.Status, to nodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		anyDepEdgeToAbsent(from, to),
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

// elementTypes returns a Type clause which binds the element var to elements of
// a specific type, filtered by the conjunction of all provided predicates.
func elementTypes(nv nodeVars, filters ...func(element scpb.Element) bool) rel.Clause {
	if len(filters) == 0 {
		panic(errors.AssertionFailedf("empty filter predicate list for var %q", nv))
	}
	var types []interface{}
	_ = forEachElement(func(e scpb.Element) error {
		for _, filter := range filters {
			if !filter(e) {
				return nil
			}
		}
		types = append(types, e)
		return nil
	})
	return nv.Type(types...)
}

func nonNilElement(element scpb.Element) scpb.Element {
	return reflect.New(reflect.ValueOf(element).Type().Elem()).Interface().(scpb.Element)
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

// Assert that only simple dependents (non-descriptor, non-index, non-column)
// have screl.ReferencedDescID attributes.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		if isSimpleDependent(e) {
			return nil
		}
		e = nonNilElement(e)
		if _, err := screl.Schema.GetAttribute(screl.ReferencedDescID, e); err == nil {
			panic(errors.AssertionFailedf("%T not expected to have screl.ReferencedDescID attr", e))
		}
		return nil
	})
}

// Assert that elements can be grouped into three categories when transitioning
// from PUBLIC to ABSENT:
// - go via DROPPED iff they're descriptor elements
// - go via a non-read status iff they're indexes or columns, which are
//   subject to the two-version invariant.
// - go direct to ABSENT in all other cases.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		s0 := opgen.InitialStatus(e, scpb.Status_ABSENT)
		s1 := opgen.NextStatus(e, scpb.Status_ABSENT, s0)
		switch s1 {
		case scpb.Status_OFFLINE, scpb.Status_DROPPED:
			if IsDescriptor(e) {
				return nil
			}
		case scpb.Status_VALIDATED, scpb.Status_WRITE_ONLY, scpb.Status_DELETE_ONLY:
			if isSubjectTo2VersionInvariant(e) {
				return nil
			}
		case scpb.Status_ABSENT:
			if isSimpleDependent(e) {
				return nil
			}
		}
		panic(errors.AssertionFailedf(
			"unexpected transition %s -> %s in direction ABSENT for %T (descriptor=%v, 2VI=%v)",
			s0, s1, e, IsDescriptor(e), isSubjectTo2VersionInvariant(e),
		))
	})
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

// Assert that isWithTypeT covers all elements with embedded TypeTs.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		e = nonNilElement(e)
		return screl.WalkTypes(e, func(t *types.T) error {
			if isWithTypeT(e) {
				return nil
			}
			panic(errors.AssertionFailedf("getTypeT should support %T but doesn't", e))
		})
	})
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

// Assert that isWithExpression covers all elements with embedded
// expressions.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		return screl.WalkExpressions(e, func(t *catpb.Expression) error {
			if isWithExpression(e) {
				return nil
			}
			panic(errors.AssertionFailedf("getExpression should support %T but doesn't", e))
		})
	})
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

// Assert that isColumnDependent covers all dependent elements of a column
// element.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		// Exclude columns themselves.
		switch e.(type) {
		case *scpb.Column:
			return nil
		}
		// A column dependent should have a ColumnID attribute.
		e = nonNilElement(e)
		_, err := screl.Schema.GetAttribute(screl.ColumnID, e)
		if isColumnDependent(e) {
			if err != nil {
				panic(err)
			}
		} else if err == nil {
			panic(errors.AssertionFailedf("isColumnDependent should include %T but doesn't", e))
		}
		return nil
	})
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

// Assert that isIndexDependent covers all dependent elements of an index
// element.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		// Exclude indexes themselves.
		if isIndex(e) {
			return nil
		}
		// An index dependent should have an IndexID attribute.
		e = nonNilElement(e)
		_, err := screl.Schema.GetAttribute(screl.IndexID, e)
		if isIndexDependent(e) {
			if err != nil {
				panic(err)
			}
		} else if err == nil {
			panic(errors.AssertionFailedf("isIndexDependent should include %T but doesn't", e))
		}
		return nil
	})
}
