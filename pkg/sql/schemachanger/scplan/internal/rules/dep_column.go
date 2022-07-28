// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// These rules ensure that column-dependent elements, like a column's name, its
// DEFAULT expression, etc. appear once the column reaches a suitable state.
// Vice-versa for column removal.
func init() {
	registerDepRule(
		"column name set right after column existence",
		scgraph.SameStagePrecedence,
		"column", "column-name",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type((*scpb.ColumnName)(nil)),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_PUBLIC),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
			}
		},
	)
	registerDepRule(
		"column existence precedes column dependents",
		scgraph.Precedence,
		"column", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type(
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
					(*scpb.ColumnComment)(nil),
					(*scpb.IndexColumn)(nil),
				),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_DELETE_ONLY),
				currentStatus(to.node, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"DEFAULT or ON UPDATE existence precedes writes to column",
		scgraph.Precedence,
		"expr", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type(
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
				),
				to.el.Type((*scpb.Column)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatus(from.node, scpb.Status_PUBLIC),
				currentStatus(to.node, scpb.Status_WRITE_ONLY),
			}
		},
	)

	// TODO(ajwerner): Understand this rule and why it needs to exist.
	registerDepRule(
		"column named before column type becomes public",
		scgraph.Precedence,
		"column-name", "column-type",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.ColumnName)(nil)),
				to.el.Type((*scpb.ColumnType)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatusEq(from.node, to.node, scpb.Status_PUBLIC),
			}
		},
	)

	// The comment is all that is remaining column dependents now that the name
	// and the DEFAULT and ON UPDATE expressions have already been dealt with.
	registerDepRule(
		"column comment exists before column becomes public",
		scgraph.Precedence,
		"column-comment", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.ColumnComment)(nil)),
				to.el.Type((*scpb.Column)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				currentStatusEq(from.node, to.node, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"column dependents removed after column no longer public",
		scgraph.Precedence,
		"column", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type(
					(*scpb.ColumnType)(nil),
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnComment)(nil),
				),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatus(from.node, scpb.Status_WRITE_ONLY),
				currentStatus(to.node, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"column type dependents removed right before column type",
		scgraph.SameStagePrecedence,
		"dependent", "column-type",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type(
					(*scpb.SequenceOwner)(nil),
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
				),
				to.el.Type((*scpb.ColumnType)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"dependents removed before column",
		scgraph.Precedence,
		"dependent", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.el.Type(
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnType)(nil),
					(*scpb.ColumnComment)(nil),
				),
				to.el.Type((*scpb.Column)(nil)),
				joinOnColumnID(from.el, to.el, "table-id", "col-id"),
				targetStatusEq(from.target, to.target, scpb.ToAbsent),
				currentStatusEq(from.node, to.node, scpb.Status_ABSENT),
			}
		},
	)
}

// This rule ensures that columns depend on each other in increasing order.
func init() {
	registerDepRule(
		"ensure columns are in increasing order",
		scgraph.SameStagePrecedence,
		"later-column", "earlier-column",
		func(from, to nodeVars) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.el.Type((*scpb.Column)(nil)),
				to.el.Type((*scpb.Column)(nil)),
				join(from.el, to.el, screl.DescID, "table-id"),
				targetStatusEq(from.target, to.target, scpb.ToPublic),
				status.In(scpb.Status_WRITE_ONLY, scpb.Status_PUBLIC),
				status.Entities(screl.CurrentStatus, from.node, to.node),
				rel.Filter("columnHasSmallerID", from.el, to.el)(func(
					from *scpb.Column, to *scpb.Column,
				) bool {
					return from.ColumnID < to.ColumnID
				}),
			}
		})
}
