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

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

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

func toPublicOrTransient(from, to nodeVars) rel.Clause {
	return toPublicOrTransientUntyped(from.target, to.target)
}

func statusesToPublicOrTransient(
	from nodeVars, fromStatus scpb.Status, to nodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		toPublicOrTransient(from, to),
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

func transient(from, to nodeVars) rel.Clause {
	return transientUntyped(from.target, to.target)
}

func statusesTransient(
	from nodeVars, fromStatus scpb.Status, to nodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		transient(from, to),
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

func columnInSwappedInPrimaryIndex(
	indexColumn, index nodeVars, relationIDVar, columnIDVar, indexIDVar rel.Var,
) rel.Clause {
	return columnInSwappedInPrimaryIndexUntyped(indexColumn.el, index.el, relationIDVar, columnIDVar, indexIDVar)
}

var (
	toPublicOrTransientUntyped = screl.Schema.Def2(
		"toPublicOrTransient",
		"target1", "target2",
		func(target1 rel.Var, target2 rel.Var) rel.Clauses {
			return rel.Clauses{
				target1.AttrIn(screl.TargetStatus, scpb.Status_PUBLIC, scpb.Status_TRANSIENT_ABSENT),
				target2.AttrIn(screl.TargetStatus, scpb.Status_PUBLIC, scpb.Status_TRANSIENT_ABSENT),
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

	transientUntyped = screl.Schema.Def2(
		"transient",
		"target1", "target2",
		func(target1 rel.Var, target2 rel.Var) rel.Clauses {
			return rel.Clauses{
				target1.AttrEq(screl.TargetStatus, scpb.Status_TRANSIENT_ABSENT),
				target2.AttrEq(screl.TargetStatus, scpb.Status_TRANSIENT_ABSENT),
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

	sourceIndexIsSetUntyped = screl.Schema.Def1("sourceIndexIsSet", "index", func(
		index rel.Var,
	) rel.Clauses {
		return rel.Clauses{
			index.AttrNeq(screl.SourceIndexID, catid.IndexID(0)),
		}
	})

	columnInSwappedInPrimaryIndexUntyped = screl.Schema.Def5(
		"columnInSwappedInPrimaryIndex",
		"index-column", "index", "table-id", "column-id", "index-id", func(
			indexColumn, index, tableID, columnID, indexID rel.Var,
		) rel.Clauses {
			return rel.Clauses{
				columnInIndexUntyped(
					indexColumn, index, tableID, columnID, indexID,
				),
				sourceIndexIsSetUntyped(index),
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
	case *scpb.Database, *scpb.Schema, *scpb.Table, *scpb.View, *scpb.Sequence,
		*scpb.AliasType, *scpb.EnumType, *scpb.CompositeType:
		return true
	}
	return false
}

func isSubjectTo2VersionInvariant(e scpb.Element) bool {
	// TODO(ajwerner): This should include constraints and enum values but it
	// currently does not because we do not support dropping them unless we're
	// dropping the descriptor and we do not support adding them.
	return isIndex(e) || isColumn(e) || isSupportedNonIndexBackedConstraint(e)
}

func isIndex(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	}
	return false
}

func isColumn(e scpb.Element) bool {
	_, ok := e.(*scpb.Column)
	return ok
}

func isSimpleDependent(e scpb.Element) bool {
	return !IsDescriptor(e) && !isSubjectTo2VersionInvariant(e) && !isData(e)
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

func isTypeDescriptor(element scpb.Element) bool {
	switch element.(type) {
	case *scpb.EnumType, *scpb.AliasType, *scpb.CompositeType:
		return true
	default:
		return false
	}
}

func isColumnDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ColumnType, *scpb.ColumnNotNull:
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

// A non-index-backed constraint is one of {Check, FK, UniqueWithoutIndex}. We only
// support Check for now.
// TODO (xiang): Expand this predicate to include other non-index-backed constraints
// when we properly support adding/dropping them in the new schema changer.
func isSupportedNonIndexBackedConstraint(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.CheckConstraint, *scpb.ForeignKeyConstraint, *scpb.UniqueWithoutIndexConstraint,
		*scpb.ColumnNotNull:
		return true
	}
	return false
}

func isConstraint(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	case *scpb.CheckConstraint, *scpb.UniqueWithoutIndexConstraint, *scpb.ForeignKeyConstraint:
		return true
	}
	return false
}

func isConstraintDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ConstraintWithoutIndexName:
		return true
	case *scpb.ConstraintComment:
		return true
	}
	return false
}

func isData(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.DatabaseData:
		return true
	case *scpb.TableData:
		return true
	case *scpb.IndexData:
		return true
	}
	return false
}

type elementTypePredicate = func(e scpb.Element) bool

func or(predicates ...elementTypePredicate) elementTypePredicate {
	return func(e scpb.Element) bool {
		for _, p := range predicates {
			if p(e) {
				return true
			}
		}
		return false
	}
}

// registerDepRuleForDrop is a convenience function which calls
// registerDepRule with the cross-product of (ToAbsent,Transient)^2 target
// states, which can't easily be composed.
func registerDepRuleForDrop(
	ruleName scgraph.RuleName,
	kind scgraph.DepEdgeKind,
	from, to string,
	fromStatus, toStatus scpb.Status,
	fn func(from, to nodeVars) rel.Clauses,
) {

	transientFromStatus, okFrom := scpb.GetTransientEquivalent(fromStatus)
	if !okFrom {
		panic(errors.AssertionFailedf("Invalid 'from' status %s", fromStatus))
	}
	transientToStatus, okTo := scpb.GetTransientEquivalent(toStatus)
	if !okTo {
		panic(errors.AssertionFailedf("Invalid 'from' status %s", toStatus))
	}

	registerDepRule(ruleName, kind, from, to, func(from, to nodeVars) rel.Clauses {
		return append(
			fn(from, to),
			statusesToAbsent(from, fromStatus, to, toStatus),
		)
	})

	registerDepRule(ruleName, kind, from, to, func(from, to nodeVars) rel.Clauses {
		return append(
			fn(from, to),
			statusesTransient(from, transientFromStatus, to, transientToStatus),
		)
	})

	registerDepRule(ruleName, kind, from, to, func(from, to nodeVars) rel.Clauses {
		return append(
			fn(from, to),
			from.targetStatus(scpb.Transient),
			from.currentStatus(transientFromStatus),
			to.targetStatus(scpb.ToAbsent),
			to.currentStatus(toStatus),
		)
	})

	registerDepRule(ruleName, kind, from, to, func(from, to nodeVars) rel.Clauses {
		return append(
			fn(from, to),
			from.targetStatus(scpb.ToAbsent),
			from.currentStatus(fromStatus),
			to.targetStatus(scpb.Transient),
			to.currentStatus(transientToStatus),
		)
	})
}

// descriptorIsNotBeingDropped creates a clause which leads to the outer clause
// failing to unify if the passed element is part of a descriptor and
// that descriptor is being dropped.
var descriptorIsNotBeingDropped = screl.Schema.DefNotJoin1(
	"descriptorIsNotBeingDropped", "element", func(
		element rel.Var,
	) rel.Clauses {
		descriptor := mkNodeVars("descriptor")
		return rel.Clauses{
			descriptor.typeFilter(IsDescriptor),
			descriptor.joinTarget(),
			joinOnDescIDUntyped(descriptor.el, element, "id"),
			descriptor.targetStatus(scpb.ToAbsent),
		}
	},
)

// fromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL creates
// a clause which leads to the outer clause failing to unify
// if the passed element `from` is a Table, `to` is a RowLevelTTl,
// and there does not exist a node with the same target as
// `fromTarget` in PUBLIC status.
// It is used to suppress rule "descriptor drop right before dependent element removal"
// for the special case where we drop a rowLevelTTL table in mixed
// version state for forward compatibility (issue #86672).
var fromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL = screl.Schema.DefNotJoin3(
	"fromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL",
	"fromTarget", "fromEl", "toEl", func(fromTarget, fromEl, toEl rel.Var) rel.Clauses {
		n := rel.Var("n")
		return rel.Clauses{
			fromEl.Type((*scpb.Table)(nil)),
			toEl.Type((*scpb.RowLevelTTL)(nil)),
			n.Type((*screl.Node)(nil)),
			n.AttrEqVar(screl.Target, fromTarget),
			screl.Schema.DefNotJoin1("nodeHasNoPublicStatus", "n", func(n rel.Var) rel.Clauses {
				public := rel.Var("public")
				return rel.Clauses{
					public.Eq(scpb.Status_PUBLIC),
					n.AttrEqVar(screl.CurrentStatus, public),
				}
			})(n),
		}
	})
