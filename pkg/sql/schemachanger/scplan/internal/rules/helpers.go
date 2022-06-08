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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
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
	toAbsentInDropped    = toAbsentIn(scpb.Status_DROPPED)
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
)
