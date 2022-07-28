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
	"fmt"
	"reflect"
	"strings"

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

func currentStatus(node rel.Var, status scpb.Status) rel.Clause {
	return node.AttrEq(screl.CurrentStatus, status)
}

func targetStatus(target rel.Var, status scpb.TargetStatus) rel.Clause {
	return target.AttrEq(screl.TargetStatus, status.Status())
}

func targetStatusEq(aTarget, bTarget rel.Var, status scpb.TargetStatus) rel.Clause {
	return rel.And(targetStatus(aTarget, status), targetStatus(bTarget, status))
}

func currentStatusEq(aNode, bNode rel.Var, status scpb.Status) rel.Clause {
	return rel.And(currentStatus(aNode, status), currentStatus(bNode, status))
}

func join(a, b rel.Var, attr rel.Attr, eqVarName rel.Var) rel.Clause {
	return joinOn(a, attr, b, attr, eqVarName)
}

func joinOn(a rel.Var, aAttr rel.Attr, b rel.Var, bAttr rel.Attr, eqVarName rel.Var) rel.Clause {
	return rel.And(
		a.AttrEqVar(aAttr, eqVarName),
		b.AttrEqVar(bAttr, eqVarName),
	)
}

var (
	toAbsent = screl.Schema.Def2(
		"toAbsent",
		"target1", "target2",
		func(target1 rel.Var, target2 rel.Var) rel.Clauses {
			return rel.Clauses{
				targetStatusEq(target1, target2, scpb.ToAbsent),
			}
		})

	toAbsentIn = func(status scpb.Status) rel.Rule4 {
		ss := status.String()
		return screl.Schema.Def4(
			fmt.Sprintf("toAbsentIn%s%s", ss[0:1], strings.ToLower(ss[1:])),
			"target1", "node1", "target2", "node2",
			func(
				target1 rel.Var, node1 rel.Var, target2 rel.Var, node2 rel.Var,
			) rel.Clauses {
				return rel.Clauses{
					toAbsent(target1, target2),
					currentStatusEq(node1, node2, status),
				}
			})
	}
	toAbsentInAbsent     = toAbsentIn(scpb.Status_ABSENT)
	joinReferencedDescID = screl.Schema.Def3(
		"joinReferencedDescID", "referrer", "referenced", "id", func(
			referrer, referenced, id rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				referrer.AttrEqVar(screl.ReferencedDescID, id),
				referenced.AttrEqVar(screl.DescID, id),
			}
		})
	joinOnDescID = screl.Schema.Def3(
		"joinOnDescID", "a", "b", "id", func(
			a, b, id rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				id.Entities(screl.DescID, a, b),
			}
		})
	joinOnIndexID = screl.Schema.Def4(
		"joinOnIndexID", "a", "b", "desc-id", "index-id", func(
			a, b, descID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				joinOnDescID(a, b, descID),
				indexID.Entities(screl.IndexID, a, b),
			}
		},
	)
	joinOnColumnID = screl.Schema.Def4(
		"joinOnColumnID", "a", "b", "desc-id", "col-id", func(
			a, b, descID, colID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				joinOnDescID(a, b, descID),
				colID.Entities(screl.ColumnID, a, b),
			}
		},
	)
	joinOnConstraintID = screl.Schema.Def4(
		"joinOnConstraintID", "a", "b", "desc-id", "constraint-id", func(
			a, b, descID, constraintID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				joinOnDescID(a, b, descID),
				constraintID.Entities(screl.ConstraintID, a, b),
			}
		},
	)
	indexDependents = screl.Schema.Def4("index-dependents",
		"index", "dep",
		"table-id", "index-id", func(
			index, dep, tableID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				dep.Type(
					(*scpb.IndexName)(nil),
					(*scpb.IndexPartitioning)(nil),
					(*scpb.SecondaryIndexPartial)(nil),
					(*scpb.IndexComment)(nil),
					(*scpb.IndexColumn)(nil),
				),
				index.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.TemporaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				joinOnIndexID(dep, index, "table-id", "index-id"),
			}
		})

	indexContainsColumn = screl.Schema.Def6(
		"indexContainsColumn",
		"index", "column", "index-column", "table-id", "column-id", "index-id", func(
			index, column, indexColumn, tableID, columnID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				index.AttrEqVar(screl.IndexID, indexID),
				indexColumn.Type((*scpb.IndexColumn)(nil)),
				indexColumn.AttrEqVar(screl.DescID, rel.Blank),
				joinOnColumnID(column, indexColumn, tableID, columnID),
				joinOnIndexID(index, indexColumn, tableID, indexID),
			}
		})

	sourceIndexNotSet = screl.Schema.Def1("sourceIndexNotSet", "index", func(
		index rel.Var,
	) rel.Clauses {
		return rel.Clauses{
			index.AttrNeq(screl.SourceIndexID, catid.IndexID(0)),
		}
	})

	columnInPrimaryIndexSwap = screl.Schema.Def6(
		"columnInPrimaryIndexSwap",
		"index", "column", "index-column", "table-id", "column-id", "index-id", func(
			index, column, indexColumn, tableID, columnID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				indexContainsColumn(
					index, column, indexColumn, tableID, columnID, indexID,
				),
				sourceIndexNotSet(index),
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
	if len(types) == 0 {
		panic(errors.AssertionFailedf("empty type list for var %q", nv))
	}
	return nv.el.Type(types[0], types[1:]...)
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
	switch e.(type) {
	case *scpb.Column, *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
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
