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

func targetNodeVars(el rel.Var) (element, target, node rel.Var) {
	return el, el + "-target", el + "-node"
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

func joinOnIndexID(a, b rel.Var) rel.Clause {
	return rel.And(
		join(a, b, screl.DescID, "desc-id"),
		join(a, b, screl.IndexID, "index-id"),
	)
}

func joinOnColumnID(a, b rel.Var) rel.Clause {
	return rel.And(
		join(a, b, screl.DescID, "desc-id"),
		join(a, b, screl.ColumnID, "column-id"),
	)
}
