// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_25_1

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

// These rules ensure that index-dependent elements, like an index's name, its
// partitioning, etc. disappear once the index reaches a suitable state.
func init() {

	// For a column to be removed from an index, the index must be validated,
	// which will not happen for temporary ones.
	registerDepRuleForDrop(
		"index no longer public before dependents, excluding columns",
		scgraph.Precedence,
		"index", "dependent",
		scpb.Status_VALIDATED, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isIndex),
				to.TypeFilter(rulesVersionKey, isIndexDependent, Not(isIndexColumn)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)
	// For temporary indexes we have to wait till DELETE_ONLY for primary index
	// swaps, as the temporary index transitions into a drop state. Normally these
	// get optimized out, so it should be safe to wait longer for all index types.
	registerDepRuleForDrop(
		"index drop mutation visible before cleaning up index columns",
		scgraph.Precedence,
		"index", "dependent",
		scpb.Status_DELETE_ONLY, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isIndex),
				to.TypeFilter(rulesVersionKey, isIndexColumn),
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
				to.TypeFilter(rulesVersionKey, isIndex),
				JoinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)

	// This rule helps us to have the index name inside event log entries.
	registerDepRuleForDrop(
		"index no longer public before index name",
		scgraph.Precedence,
		"index", "name",
		scpb.Status_DELETE_ONLY, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.IndexName)(nil)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)
}

// Special cases of the above.
func init() {

	// If we're going to be removing columns from an index, we know that
	// it'll be because we're dropping the index. If we're dropping the
	// index and not the descriptor, we need to make sure that we only
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
		"index-column", "index",
		scpb.Status_DELETE_ONLY, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexColumn)(nil)),
				to.TypeFilter(rulesVersionKey, isIndex),
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

func init() {
	// TODO(fqazi): We need to model these rules better to use indexes,
	// since they may perform terrible in scenarios where we are dropping
	// a large number of views and indexes (i.e. O(views * indexes) ).
	registerDepRuleForDrop(
		"dependent view no longer public before secondary index",
		scgraph.Precedence,
		"view", "index",
		scpb.Status_DROPPED, scpb.Status_VALIDATED,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.View)(nil)),
				to.Type((*scpb.SecondaryIndex)(nil)),
				FilterElements("viewReferencesIndex", from, to, func(from *scpb.View, to *scpb.SecondaryIndex) bool {
					for _, ref := range from.ForwardReferences {
						if ref.ToID == to.TableID &&
							ref.IndexID == to.IndexID {
							return true
						}
					}
					return false
				}),
			}
		},
	)
	registerDepRuleForDrop(
		"secondary index should be validated before dependent view can be absent",
		scgraph.Precedence,
		"index", "view",
		scpb.Status_VALIDATED, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.View)(nil)),
				FilterElements("viewReferencesIndex", from, to, func(from *scpb.SecondaryIndex, to *scpb.View) bool {
					for _, ref := range to.ForwardReferences {
						if ref.ToID == from.TableID &&
							ref.IndexID == from.IndexID {
							return true
						}
					}
					return false
				}),
			}
		},
	)
	registerDepRuleForDrop(
		"dependent view absent before secondary index",
		scgraph.Precedence,
		"view", "index",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.View)(nil)),
				to.Type((*scpb.SecondaryIndex)(nil)),
				FilterElements("viewReferencesIndex", from, to, func(from *scpb.View, to *scpb.SecondaryIndex) bool {
					for _, ref := range from.ForwardReferences {
						if ref.ToID == to.TableID &&
							ref.IndexID == to.IndexID {
							return true
						}
					}
					return false
				}),
			}
		},
	)
}
