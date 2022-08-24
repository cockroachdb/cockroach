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
func init() {

	registerDepRule(
		"column existence precedes column dependents",
		scgraph.Precedence,
		"column", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.typeFilter(isColumnDependent),
				joinOnColumnID(from, to, "table-id", "col-id"),
				statusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"column dependents exist before column becomes public",
		scgraph.Precedence,
		"dependent", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isColumnDependent),
				to.Type((*scpb.Column)(nil)),
				joinOnColumnID(from, to, "table-id", "col-id"),
				statusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}

// Special cases of the above.
func init() {
	registerDepRule(
		"column name and type set right after column existence",
		scgraph.SameStagePrecedence,
		"column", "column-name-or-type",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type(
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnType)(nil),
				),
				statusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_PUBLIC),
				joinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)

	registerDepRule(
		"DEFAULT or ON UPDATE existence precedes writes to column",
		scgraph.Precedence,
		"expr", "column",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
				),
				to.Type((*scpb.Column)(nil)),
				joinOnColumnID(from, to, "table-id", "col-id"),
				statusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_WRITE_ONLY),
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
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.Column)(nil)),
				joinOnDescID(from, to, "table-id"),
				toPublicOrTransient(from, to),
				status.In(scpb.Status_WRITE_ONLY, scpb.Status_PUBLIC),
				status.Entities(screl.CurrentStatus, from.node, to.node),
				filterElements("SmallerColumnIDFirst", from, to, func(from, to *scpb.Column) bool {
					return from.ColumnID < to.ColumnID
				}),
			}
		})
}
