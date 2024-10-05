// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_22_2

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

// These rules ensure that column-dependent elements, like a column's name, its
// DEFAULT expression, etc. disappear once the column reaches a suitable state.
func init() {

	registerDepRuleForDrop(
		"column no longer public before dependents",
		scgraph.Precedence,
		"column", "dependent",
		scpb.Status_WRITE_ONLY, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.TypeFilter(rulesVersionKey, isColumnDependent),
				JoinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)

	registerDepRuleForDrop(
		"dependents removed before column",
		scgraph.Precedence,
		"dependent", "column",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isColumnDependent),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)
}

// Special cases of the above.
func init() {

	registerDepRule(
		"column type dependents removed right before column type",
		scgraph.SameStagePrecedence,
		"dependent", "column-type",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isColumnTypeDependent),
				to.Type((*scpb.ColumnType)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		},
	)

	// Special cases for removal of column types, which hold references to other
	// descriptors.
	//
	// When the whole table is dropped, we can (and in fact, should) remove these
	// right away in-txn. However, when only the column is dropped but the table
	// remains, we need to wait until the column is DELETE_ONLY, which happens
	// post-commit because of the need to uphold the 2-version invariant.
	//
	// We distinguish the two cases using a flag in ColumnType which is set iff
	// the parent relation is dropped. This is a dirty hack, ideally we should be
	// able to express the _absence_ of a target element as a query clause.
	//
	// Note that DEFAULT and ON UPDATE expressions are column-dependent elements
	// which also hold references to other descriptors. The rule prior to this one
	// ensures that they transition to ABSENT before scpb.ColumnType does.
	registerDepRule(
		"column type removed right before column when not dropping relation",
		scgraph.SameStagePrecedence,
		"column-type", "column",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnType)(nil)),
				descriptorIsNotBeingDropped(from.El),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		},
	)

}
