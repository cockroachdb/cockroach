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

// These rules ensure that index-dependent elements, like an index's name, its
// partitioning, etc. disappear once the index reaches a suitable state.
func init() {

	registerDepRuleForDrop(
		"index no longer public before dependents",
		scgraph.Precedence,
		"index", "dependent",
		scpb.Status_VALIDATED, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, IsIndex),
				to.TypeFilter(rulesVersionKey, isIndexDependent),
				JoinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)
	registerDepRuleForDrop(
		"dependents removed before index",
		scgraph.Precedence,
		"dependent", "index",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isIndexDependent),
				to.TypeFilter(rulesVersionKey, IsIndex),
				JoinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)
}

// Special cases of the above.
func init() {

	// If we're going to be removing columns from an index, we know that
	// it'll be because we're dropping the index. If we're dropping the
	// index and rules.Not the descriptor, we need to make sure that we only
	// do it once the index is definitely being dropped. The reason for
	// this is roundabout: dropping a column from an index which is itself
	// being dropped is treated as a no-op by the op rules.
	//
	// TODO(ajwerner): This rule really feels like it ought to be a
	// same stage precedence sort of rule where we remove the columns from the
	// index when we remove the index, but for some reason, that overconstrains
	// the graph when dropping the table. Because of that, we allow the column
	// to be removed from the index in DELETE_ONLY, and we no-op the removal.
	registerDepRuleForDrop(
		"remove columns from index right before removing index",
		scgraph.Precedence,
		"index", "index-column",
		scpb.Status_DELETE_ONLY, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexColumn)(nil)),
				to.TypeFilter(rulesVersionKey, IsIndex),
				JoinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)

	// Special case for removal of partial predicates, which hold references to
	// other descriptors.
	//
	// When the whole table is dropped, we can (and in fact, should) remove these
	// right away in-txn. However, when only the index is dropped but the table
	// remains, we need to wait until the index is DELETE_ONLY, which happens
	// post-commit because of the need to uphold the 2-version invariant.
	//
	// We distinguish the two cases using a flag in SecondaryIndexPartial which is
	// set iff the parent relation is dropped. This is a dirty hack, ideally we
	// should be able to express the _absence_ of a target element as a query
	// clause.
	registerDepRuleForDrop(
		"partial predicate removed right before secondary index when not dropping relation",
		scgraph.SameStagePrecedence,
		"partial-predicate", "index",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndexPartial)(nil)),
				descriptorIsNotBeingDropped(from.El),
				to.Type((*scpb.SecondaryIndex)(nil)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)
}
