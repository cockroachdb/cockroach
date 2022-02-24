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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
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

func targetNodeVars(el rel.Var) (element, target, node rel.Var) {
	return el, el + "-target", el + "-node"
}

type depRuleSpec struct {
	ruleName              string
	edgeKind              scgraph.DepEdgeKind
	targetStatus          scpb.Status
	from, to              elementSpec
	joinAttrs             []screl.Attr
	joinReferencingDescID bool
	filterLabel           string
	filter                interface{}
}

func depRule(
	ruleName string,
	edgeKind scgraph.DepEdgeKind,
	targetStatus scpb.TargetStatus,
	from, to elementSpec,
	joinAttrs ...screl.Attr,
) depRuleSpec {
	return depRuleSpec{
		ruleName:     ruleName,
		edgeKind:     edgeKind,
		targetStatus: targetStatus.Status(),
		from:         from,
		to:           to,
		joinAttrs:    joinAttrs,
	}
}

type elementSpec struct {
	status scpb.Status
	types  []interface{}
}

func element(status scpb.Status, types ...interface{}) elementSpec {
	return elementSpec{
		status: status,
		types:  types,
	}
}

func (d depRuleSpec) withFilter(label string, predicate interface{}) depRuleSpec {
	d.filterLabel = label
	d.filter = predicate
	return d
}

func (d depRuleSpec) withJoinFromReferencedDescIDWithToDescID() depRuleSpec {
	d.joinReferencingDescID = true
	return d
}

func (d depRuleSpec) register() {
	var (
		from, fromTarget, fromNode = targetNodeVars("from")
		to, toTarget, toNode       = targetNodeVars("to")
	)
	if from == to {
		panic(errors.AssertionFailedf("elements cannot share same label %q", from))
	}
	c := rel.Clauses{
		from.Type(d.from.types[0], d.from.types[1:]...),
		to.Type(d.to.types[0], d.to.types[1:]...),

		fromTarget.AttrEq(screl.TargetStatus, d.targetStatus),
		toTarget.AttrEq(screl.TargetStatus, d.targetStatus),

		fromNode.AttrEq(screl.CurrentStatus, d.from.status),
		toNode.AttrEq(screl.CurrentStatus, d.to.status),

		screl.JoinTargetNode(from, fromTarget, fromNode),
		screl.JoinTargetNode(to, toTarget, toNode),
	}
	for _, attr := range d.joinAttrs {
		v := rel.Var(attr.String() + "-join-var")
		c = append(c, from.AttrEqVar(attr, v), to.AttrEqVar(attr, v))
	}
	if d.joinReferencingDescID {
		v := rel.Var("joined-from-ref-desc-id-with-to-desc-id-var")
		c = append(c, from.AttrEqVar(screl.ReferencedDescID, v), to.AttrEqVar(screl.DescID, v))
	}
	if d.filter != nil {
		c = append(c, rel.Filter(d.filterLabel, from, to)(d.filter))
	}
	registerDepRule(d.ruleName, d.edgeKind, fromNode, toNode, screl.MustQuery(c...))
}
