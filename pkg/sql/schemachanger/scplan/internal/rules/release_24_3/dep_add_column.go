// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_24_3

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
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
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.TypeFilter(rulesVersionKey, isColumnDependent),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				StatusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"column dependents exist before column becomes public",
		scgraph.Precedence,
		"dependent", "column",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isColumnDependent),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
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
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type(
					(*scpb.ColumnName)(nil),
					(*scpb.ColumnType)(nil),
				),
				StatusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_PUBLIC),
				JoinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)

	registerDepRule(
		"DEFAULT or ON UPDATE existence precedes writes to column",
		scgraph.Precedence,
		"expr", "column",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.ColumnDefaultExpression)(nil),
					(*scpb.ColumnOnUpdateExpression)(nil),
				),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_WRITE_ONLY),
			}
		},
	)

	// Column becomes writable in the same stage as column constraint is enforced.
	//
	// This rule exists to prevent the case that the constraint becomes enforced
	// (which means writes need to honor it) when the column itself is still
	// in DELETE_ONLY and thus not visible to writes.
	//
	// N.B. It's essentially the same rule as "column constraint removed right
	// before column reaches delete only" but on the adding path.
	// N.B. SameStage is enough; which transition happens first won't matter.
	registerDepRule(
		"column writable right before column constraint is enforced.",
		scgraph.SameStagePrecedence,
		"column", "column-constraint",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.ColumnNotNull)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				StatusesToPublicOrTransient(from, scpb.Status_WRITE_ONLY, to, scpb.Status_WRITE_ONLY),
			}
		},
	)

}

// This rule ensures that columns depend on each other in increasing order.
func init() {
	registerDepRule(
		"ensure columns are in increasing order",
		scgraph.Precedence,
		"later-column", "earlier-column",
		func(from, to NodeVars) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				// Join first on the target and node to only explore all columns
				// which are being added as opposed to all columns. If we joined
				// first on the columns, we'd be filtering the cross product of
				// table columns. If a relation has a lot of columns, this can hurt.
				// It's less likely that we have a very large number of columns which
				// are being added. We'll want to do something else here when we start
				// creating tables and all the columns are being added.
				//
				// The "right" answer is to push ordering predicates into rel; it also
				// is maintaining sorted data structures.
				from.JoinTargetNode(),
				to.Type((*scpb.Column)(nil)),
				JoinOnDescID(from, to, "table-id"),
				ToPublicOrTransient(from, to),
				status.In(scpb.Status_WRITE_ONLY, scpb.Status_PUBLIC),
				status.Entities(screl.CurrentStatus, from.Node, to.Node),
				FilterElements("SmallerColumnIDFirst", from, to, func(from, to *scpb.Column) bool {
					return from.ColumnID < to.ColumnID
				}),
			}
		})
}
