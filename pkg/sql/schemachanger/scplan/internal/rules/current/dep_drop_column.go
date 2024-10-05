// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package current

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

	registerDepRule(
		"column type removed before column family",
		scgraph.Precedence,
		"column-type", "column-family",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.ColumnType)(nil)),
				to.Type((*scpb.ColumnFamily)(nil)),
				JoinOnColumnFamilyID(from, to, "table-id", "family-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
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

	// Column constraint disappears in the same stage as the column
	// becomes non-writable.
	//
	// Column constraint cannot disappear while the column is still writable
	// because we then allow incorrect writes that would violate the constraint.
	//
	// Column constraint cannot still be enforced when the column becomes
	// non-writable because an enforced constraint means writes will see and
	// attempt to uphold it but the column is no longer visible to them.
	//
	// N.B. This rule supersedes the above "dependents removed before column" rule.
	// N.B. SameStage is enough; which transition happens first won't matter.
	registerDepRuleForDrop(
		"column constraint removed right before column reaches delete only",
		scgraph.SameStagePrecedence,
		"column-constraint", "column",
		scpb.Status_ABSENT, scpb.Status_DELETE_ONLY,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, isSubjectTo2VersionInvariant),
				to.Type((*scpb.Column)(nil)),
				JoinOnColumnID(from, to, "table-id", "col-id"),
			}
		},
	)
}

// Special rules partial predicate expressions, which ensure that any columns
// used by them are not cleaned up before the partial index peredicate is
// removed.
func init() {
	registerDepRuleForDrop(
		"secondary index partial no longer public before referenced column",
		scgraph.Precedence,
		"secondary-partial-index", "column",
		scpb.Status_DELETE_ONLY, scpb.Status_WRITE_ONLY,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				JoinOnDescID(from, to, "table-id"),
				descriptorIsNotBeingDropped(from.El),
				FilterElements("secondaryIndexReferencesColumn", from, to,
					func(index *scpb.SecondaryIndex, column *scpb.Column) bool {
						if index.EmbeddedExpr == nil {
							return false
						}
						for _, refColumns := range index.EmbeddedExpr.ReferencedColumnIDs {
							if refColumns == column.ColumnID {
								return true
							}
						}
						return false
					}),
			}
		},
	)
	registerDepRuleForDrop(
		"secondary index partial no longer public before referenced column",
		scgraph.Precedence,
		"secondary-partial-index", "column",
		scpb.Status_ABSENT, scpb.Status_WRITE_ONLY,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.SecondaryIndexPartial)(nil)),
				to.Type((*scpb.Column)(nil)),
				JoinOnDescID(from, to, "table-id"),
				descriptorIsNotBeingDropped(from.El),
				FilterElements("secondaryIndexReferencesColumn", from, to,
					func(index *scpb.SecondaryIndexPartial, column *scpb.Column) bool {
						for _, refColumns := range index.ReferencedColumnIDs {
							if refColumns == column.ColumnID {
								return true
							}
						}
						return false
					}),
			}
		},
	)
}
