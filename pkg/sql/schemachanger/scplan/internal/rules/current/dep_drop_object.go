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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// These rules ensure that:
//   - a descriptor reaches ABSENT in a different transaction than it reaches
//     DROPPED (i.e. it cannot be removed until PostCommit).
//   - a descriptor element reaches the DROPPED state in the txn before
//     its dependent elements (namespace entry, comments, column names, etc)
//     reach the ABSENT state;
//   - or the WRITE_ONLY state for those dependent elements subject to the
//     2-version invariant.
func init() {

	registerDepRule(
		"descriptor dropped in transaction before removal",
		scgraph.PreviousTransactionPrecedence,
		"dropped", "absent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				from.El.AttrEqVar(screl.DescID, "_"),
				from.El.AttrEqVar(rel.Self, to.El),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		})

	registerDepRule(
		"descriptor dropped before dependent element removal",
		scgraph.Precedence,
		"descriptor", "dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.TypeFilter(rulesVersionKey, Or(isSimpleDependent, isOwner), Not(isConstraintDependent)),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		})

	registerDepRule(
		"relation dropped before dependent column",
		scgraph.Precedence,
		"descriptor", "column",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Table)(nil), (*scpb.View)(nil), (*scpb.Sequence)(nil)),
				to.TypeFilter(rulesVersionKey, isColumn),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_WRITE_ONLY),
			}
		})

	registerDepRule(
		"relation dropped before dependent index",
		scgraph.Precedence,
		"descriptor", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Table)(nil), (*scpb.View)(nil)),
				to.TypeFilter(rulesVersionKey, isIndex),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_VALIDATED),
			}
		},
	)

	registerDepRule(
		"relation dropped before dependent constraint",
		scgraph.Precedence,
		"descriptor", "constraint",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Table)(nil)),
				to.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, isSubjectTo2VersionInvariant, Not(isNonIndexBackedCrossDescriptorConstraint)),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_VALIDATED),
			}
		},
	)

}

// These rules ensure that cross-referencing simple dependent elements reach
// ABSENT in the same stage right after the referenced descriptor element
// reaches DROPPED.
//
// References from simple dependent elements to other descriptors exist as
// follows:
// - simple dependent elements with a ReferencedDescID attribute,
// - those which embed a TypeT,
// - those which embed an Expression.
func init() {

	registerDepRule(
		"descriptor drop right before removing dependent with attr ref",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-attr",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.TypeFilter(rulesVersionKey, isSimpleDependent, Not(isDescriptorParentReference)),
				JoinReferencedDescID(to, from, "desc-id"),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		},
	)

	// If the descriptor references this type is already being dropped, then
	// the back references don't really matter.
	registerDepRule(
		"descriptor drop right before removing dependent between types",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-type",
		func(from, to NodeVars) rel.Clauses {
			fromDescID := rel.Var("fromDescID")
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isTypeDescriptor),
				from.DescIDEq(fromDescID),
				to.ReferencedTypeDescIDsContain(fromDescID),
				to.TypeFilter(rulesVersionKey, isSimpleDependent, isWithTypeT),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		},
	)
	registerDepRule(
		"descriptor drop right before removing dependent with type refs in expressions",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-type",
		func(from, to NodeVars) rel.Clauses {
			fromDescID := rel.Var("fromDescID")
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isTypeDescriptor),
				from.DescIDEq(fromDescID),
				to.ReferencedTypeDescIDsContain(fromDescID),
				descriptorIsNotBeingDropped(to.El),
				to.TypeFilter(rulesVersionKey, isSimpleDependent, isWithExpression),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"descriptor drop right before removing dependent with expr ref to sequence",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-expr",
		func(from, to NodeVars) rel.Clauses {
			seqID := rel.Var("seqID")
			return rel.Clauses{
				from.Type((*scpb.Sequence)(nil)),
				from.DescIDEq(seqID),
				to.ReferencedSequenceIDsContains(seqID),
				to.TypeFilter(rulesVersionKey, isSimpleDependent, isWithExpression),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"descriptor drop right before removing dependent with function refs in columns",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-function",
		func(from, to NodeVars) rel.Clauses {
			fromDescID := rel.Var("fromDescID")
			return rel.Clauses{
				from.Type((*scpb.Function)(nil)),
				from.DescIDEq(fromDescID),
				to.ReferencedFunctionIDsContains(fromDescID),
				to.TypeFilter(rulesVersionKey, isSimpleDependent, isWithExpression),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		},
	)

}

// These rules ensure that descriptor, back-reference in parent descriptor,
// and parent descriptor are dropped in appropriate order.
func init() {

	// We don't like those parent-descriptor-back-reference elements: in hindsight,
	// we shouldn't have them in the first place because we cannot modify
	// back-references in parent descriptor in isolation with the SQL syntax.
	// This rule is to deal with this fact by tightly coupling them to the descriptor.
	registerDepRule(
		"descriptor dropped right before removing back-reference in its parent descriptor",
		scgraph.SameStagePrecedence,
		"descriptor", "back-reference-in-parent-descriptor",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.TypeFilter(rulesVersionKey, isDescriptorParentReference),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		})

	registerDepRule(
		"back-reference in parent descriptor is removed before parent descriptor is dropped",
		scgraph.Precedence,
		"back-reference-in-parent-descriptor", "parent-descriptor",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptorParentReference),
				to.TypeFilter(rulesVersionKey, isDescriptor),
				JoinReferencedDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_DROPPED),
			}
		},
	)
}

// These rules ensure that dependents get removed before the descriptor.
// Some operations might require the descriptor to actually be present.
func init() {
	registerDepRule(
		"non-data dependents removed before descriptor",
		scgraph.Precedence,
		"dependent", "descriptor",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, Not(isDescriptor), Not(isData)),
				to.TypeFilter(rulesVersionKey, isDescriptor),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
			}
		})
}

// These rules ensures we drop cross-descriptor constraints before dropping
// descriptors, both the referencing and referenced. Namely,
//  1. cross-descriptor constraints are absent before referenced descriptor, if
//     the referencing table is not being dropped.
//  2. cross-descriptor constraints are absent before referencing descriptor, if
//     the referenced table is not dropped.
//
// A canonical example is FKs:
// To illustrate why rule 1 is necessary, consider we have tables `t1` and `t2`,
// and `t1` has a FK to `t2` (call this schema `S1`). The statement is
// `DROP TABLE t2 CASCADE`. We will have to first transition the FK (dropped as
// a result of CASCADE) to an intermediate state and then (in a separate
// transaction) transition the table to the dropped state. Otherwise, if the FK
// transition to absent in the same transaction as the table becomes dropped
// (call this schema `S2`), it becomes unsafe for `S1` and `S2` to exist in the
// cluster at the same time, because allowed inserts under `S2` will violate `S1`.
//
// To illustrate why rule 2 is necessary, consider we have tables `t1`, `t2`, `t3`,
// and `t1` FKs to `t2` (call it `FK1`) and `t3` FKs to `t2` (call it `FK2`).
// The statement is `DROP TABLE t1, t2 CASCADE`. Without rule 2, rule 1 alone will
// ensure that `FK2` moves to an intermediate state first, and at the same stage,
// `t1` will be dropped together with `FK1`. Validation will then fail because
// `t2` will have an enforced FK constraint whose origin table (`t1`) is dropped.
// It's worth noting that relaxing validation in this case is safe but we choose
// not to do so because it requires other related changes and makes reasoning
// harder.
func init() {
	registerDepRule(
		"cross-descriptor constraint is absent before referenced descriptor is dropped",
		scgraph.Precedence,
		"cross-desc-constraint", "referenced-descriptor",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isNonIndexBackedCrossDescriptorConstraint, isSubjectTo2VersionInvariant),
				to.TypeFilter(rulesVersionKey, isDescriptor),
				JoinReferencedDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_DROPPED),
			}
		},
	)

	registerDepRule(
		"cross-descriptor constraint is absent before referencing descriptor is dropped",
		scgraph.Precedence,
		"cross-desc-constraint", "referencing-descriptor",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isNonIndexBackedCrossDescriptorConstraint, isSubjectTo2VersionInvariant),
				to.TypeFilter(rulesVersionKey, isDescriptor),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_DROPPED),
			}
		},
	)
}
