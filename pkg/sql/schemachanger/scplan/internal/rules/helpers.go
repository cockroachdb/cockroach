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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

func join(a, b NodeVars, attr rel.Attr, eqVarName rel.Var) rel.Clause {
	return JoinOn(a, attr, b, attr, eqVarName)
}

var _ = join

func JoinOn(a NodeVars, aAttr rel.Attr, b NodeVars, bAttr rel.Attr, eqVarName rel.Var) rel.Clause {
	return rel.And(
		a.El.AttrEqVar(aAttr, eqVarName),
		b.El.AttrEqVar(bAttr, eqVarName),
	)
}

func FilterElements(name string, a, b NodeVars, fn interface{}) rel.Clause {
	return rel.Filter(name, a.El, b.El)(fn)
}

func ToPublicOrTransient(from, to NodeVars) rel.Clause {
	return toPublicOrTransientUntyped(from.Target, to.Target)
}

func StatusesToPublicOrTransient(
	from NodeVars, fromStatus scpb.Status, to NodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		ToPublicOrTransient(from, to),
		from.CurrentStatus(fromStatus),
		to.CurrentStatus(toStatus),
	)
}

func toAbsent(from, to NodeVars) rel.Clause {
	return toAbsentUntyped(from.Target, to.Target)
}

func StatusesToAbsent(
	from NodeVars, fromStatus scpb.Status, to NodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		toAbsent(from, to),
		from.CurrentStatus(fromStatus),
		to.CurrentStatus(toStatus),
	)
}

func transient(from, to NodeVars) rel.Clause {
	return transientUntyped(from.Target, to.Target)
}

func StatusesTransient(
	from NodeVars, fromStatus scpb.Status, to NodeVars, toStatus scpb.Status,
) rel.Clause {
	return rel.And(
		transient(from, to),
		from.CurrentStatus(fromStatus),
		to.CurrentStatus(toStatus),
	)
}

func JoinOnDescID(a, b NodeVars, descriptorIDVar rel.Var) rel.Clause {
	return joinOnDescIDUntyped(a.El, b.El, descriptorIDVar)
}

func JoinReferencedDescID(a, b NodeVars, descriptorIDVar rel.Var) rel.Clause {
	return joinReferencedDescIDUntyped(a.El, b.El, descriptorIDVar)
}

func JoinOnColumnID(a, b NodeVars, relationIDVar, columnIDVar rel.Var) rel.Clause {
	return joinOnColumnIDUntyped(a.El, b.El, relationIDVar, columnIDVar)
}

func JoinOnIndexID(a, b NodeVars, relationIDVar, indexIDVar rel.Var) rel.Clause {
	return joinOnIndexIDUntyped(a.El, b.El, relationIDVar, indexIDVar)
}

func JoinOnConstraintID(a, b NodeVars, relationIDVar, constraintID rel.Var) rel.Clause {
	return joinOnConstraintIDUntyped(a.El, b.El, relationIDVar, constraintID)
}

func ColumnInIndex(
	indexColumn, index NodeVars, relationIDVar, columnIDVar, indexIDVar rel.Var,
) rel.Clause {
	return columnInIndexUntyped(indexColumn.El, index.El, relationIDVar, columnIDVar, indexIDVar)
}

func ColumnInSwappedInPrimaryIndex(
	indexColumn, index NodeVars, relationIDVar, columnIDVar, indexIDVar rel.Var,
) rel.Clause {
	return columnInSwappedInPrimaryIndexUntyped(indexColumn.El, index.El, relationIDVar, columnIDVar, indexIDVar)
}

var (
	toPublicOrTransientUntyped = screl.Schema.Def2(
		"ToPublicOrTransient",
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
		"ColumnInIndex",
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
		"ColumnInSwappedInPrimaryIndex",
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

func ForEachElement(fn func(element scpb.Element) error) error {
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

func ForEachElementInActiveVersion(
	fn func(element scpb.Element) error, version clusterversion.ClusterVersion,
) error {
	var ep scpb.ElementProto
	vep := reflect.ValueOf(ep)
	for i := 0; i < vep.NumField(); i++ {
		e := vep.Field(i).Interface().(scpb.Element)
		if version.IsActive(screl.MinVersion(e)) {
			if err := fn(e); err != nil {
				return iterutil.Map(err)
			}
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

func IsSubjectTo2VersionInvariant(e scpb.Element) bool {
	// TODO(ajwerner): This should include constraints and enum values but it
	// currently does not because we do not support dropping them unless we're
	// dropping the descriptor and we do not support adding them.
	return IsIndex(e) || IsColumn(e) || IsSupportedNonIndexBackedConstraint(e)
}

func IsIndex(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	}
	return false
}

func IsColumn(e scpb.Element) bool {
	_, ok := e.(*scpb.Column)
	return ok
}

func IsSimpleDependent(e scpb.Element) bool {
	return !IsDescriptor(e) && !IsSubjectTo2VersionInvariant(e) && !IsData(e)
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

func IsWithTypeT(element scpb.Element) bool {
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

func IsWithExpression(element scpb.Element) bool {
	_, err := getExpression(element)
	return err == nil
}

func IsTypeDescriptor(element scpb.Element) bool {
	switch element.(type) {
	case *scpb.EnumType, *scpb.AliasType, *scpb.CompositeType:
		return true
	default:
		return false
	}
}

func IsColumnDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ColumnType:
		return true
	case *scpb.ColumnName, *scpb.ColumnComment, *scpb.IndexColumn:
		return true
	}
	return IsColumnTypeDependent(e)
}

func IsColumnTypeDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.SequenceOwner, *scpb.ColumnDefaultExpression, *scpb.ColumnOnUpdateExpression:
		return true
	}
	return false
}

func IsIndexDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.IndexName, *scpb.IndexComment, *scpb.IndexColumn:
		return true
	case *scpb.IndexPartitioning, *scpb.SecondaryIndexPartial:
		return true
	}
	return false
}

// IsSupportedNonIndexBackedConstraint a non-index-backed constraint is one of {Check, FK, UniqueWithoutIndex}. We only
// support Check for now.
// TODO (xiang): Expand this predicate to include other non-index-backed constraints
// when we properly support adding/dropping them in the new schema changer.
func IsSupportedNonIndexBackedConstraint(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.CheckConstraint, *scpb.ForeignKeyConstraint, *scpb.UniqueWithoutIndexConstraint:
		return true
	}
	return false
}

func IsConstraint(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	case *scpb.CheckConstraint, *scpb.UniqueWithoutIndexConstraint, *scpb.ForeignKeyConstraint:
		return true
	}
	return false
}

func IsConstraintDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ConstraintWithoutIndexName:
		return true
	case *scpb.ConstraintComment:
		return true
	}
	return false
}

func IsData(e scpb.Element) bool {
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

func Or(predicates ...elementTypePredicate) elementTypePredicate {
	return func(e scpb.Element) bool {
		for _, p := range predicates {
			if p(e) {
				return true
			}
		}
		return false
	}
}

func Not(predicate elementTypePredicate) elementTypePredicate {
	return func(e scpb.Element) bool {
		return !predicate(e)
	}
}

// RegisterDepRuleForDrop is a convenience function which calls
// RegisterDepRule with the cross-product of (ToAbsent,Transient)^2 Target
// states, which can't easily be composed.
func RegisterDepRuleForDrop(
	r *Registry,
	ruleName scgraph.RuleName,
	kind scgraph.DepEdgeKind,
	from, to string,
	fromStatus, toStatus scpb.Status,
	fn func(from, to NodeVars) rel.Clauses,
) {

	transientFromStatus, okFrom := scpb.GetTransientEquivalent(fromStatus)
	if !okFrom {
		panic(errors.AssertionFailedf("Invalid 'from' status %s", fromStatus))
	}
	transientToStatus, okTo := scpb.GetTransientEquivalent(toStatus)
	if !okTo {
		panic(errors.AssertionFailedf("Invalid 'from' status %s", toStatus))
	}

	r.RegisterDepRule(ruleName, kind, from, to, func(from, to NodeVars) rel.Clauses {
		return append(
			fn(from, to),
			StatusesToAbsent(from, fromStatus, to, toStatus),
		)
	})

	r.RegisterDepRule(ruleName, kind, from, to, func(from, to NodeVars) rel.Clauses {
		return append(
			fn(from, to),
			StatusesTransient(from, transientFromStatus, to, transientToStatus),
		)
	})

	r.RegisterDepRule(ruleName, kind, from, to, func(from, to NodeVars) rel.Clauses {
		return append(
			fn(from, to),
			from.TargetStatus(scpb.Transient),
			from.CurrentStatus(transientFromStatus),
			to.TargetStatus(scpb.ToAbsent),
			to.CurrentStatus(toStatus),
		)
	})

	r.RegisterDepRule(ruleName, kind, from, to, func(from, to NodeVars) rel.Clauses {
		return append(
			fn(from, to),
			from.TargetStatus(scpb.ToAbsent),
			from.CurrentStatus(fromStatus),
			to.TargetStatus(scpb.Transient),
			to.CurrentStatus(transientToStatus),
		)
	})
}

// descriptorIsNotBeingDropped creates a clause which leads to the outer clause
// failing to unify if the passed element is part of a descriptor and
// that descriptor is being dropped.
var descriptorIsNotBeingDropped = screl.Schema.DefNotJoin1(
	"DescriptorIsNotBeingDropped", "element", func(
		element rel.Var,
	) rel.Clauses {
		descriptor := MkNodeVars("descriptor")
		return rel.Clauses{
			descriptor.TypeFilter(IsDescriptor),
			descriptor.JoinTarget(),
			joinOnDescIDUntyped(descriptor.El, element, "id"),
			descriptor.TargetStatus(scpb.ToAbsent),
		}
	},
)

// FromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL creates
// a clause which leads to the outer clause failing to unify
// if the passed element `from` is a Table, `to` is a RowLevelTTl,
// and there does not exist a Node with the same Target as
// `fromTarget` in PUBLIC status.
// It is used to suppress rule "descriptor drop right before dependent element removal"
// for the special case where we drop a rowLevelTTL table in mixed
// version state for forward compatibility (issue #86672).
var FromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL = screl.Schema.DefNotJoin3(
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

// notJoinOnNodeWithStatusIn is a cache to memoize getNotJoinOnNodeWithStatusIn.
var notJoinOnNodeWithStatusIn = map[string]rel.Rule1{}

// GetNotJoinOnNodeWithStatusIn returns a not-join rule which takes a variable
// corresponding to a target in the graph as input and will exclude that target
// if the graph contains a node with that target in any of the listed status
// values.
func GetNotJoinOnNodeWithStatusIn(statues []scpb.Status) rel.Rule1 {
	makeStatusesStrings := func(statuses []scpb.Status) []string {
		ret := make([]string, len(statuses))
		for i, status := range statuses {
			ret[i] = status.String()
		}
		return ret
	}
	makeStatusesString := func(statuses []scpb.Status) string {
		return strings.Join(makeStatusesStrings(statuses), "_")
	}
	boxStatuses := func(input []scpb.Status) []interface{} {
		ret := make([]interface{}, len(input))
		for i, s := range input {
			ret[i] = s
		}
		return ret
	}
	name := makeStatusesString(statues)
	if got, ok := notJoinOnNodeWithStatusIn[name]; ok {
		return got
	}
	r := screl.Schema.DefNotJoin1(
		fmt.Sprintf("nodeNotExistsWithStatusIn_%s", name),
		"sharedTarget", func(target rel.Var) rel.Clauses {
			n := rel.Var("n")
			return rel.Clauses{
				n.Type((*screl.Node)(nil)),
				n.AttrEqVar(screl.Target, target),
				n.AttrIn(screl.CurrentStatus, boxStatuses(statues)...),
			}
		})
	notJoinOnNodeWithStatusIn[name] = r
	return r
}
