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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// These rules ensure that:
//   - a descriptor reaches the TXN_DROPPED state in the statement phase, and
//     it does rules.Not reach DROPPED until the pre-commit phase.
//   - a descriptor reaches ABSENT in a different transaction than it reaches
//     DROPPED (i.e. it canrules.Not be removed until PostCommit).
//   - a descriptor element reaches the DROPPED state in the txn before
//     its dependent elements (namespace entry, comments, column names, etc) reach
//     the ABSENT state;
//   - for those dependent elements which have to wait post-commit to reach the
//     ABSENT state, we tie them to the same stage as when the descriptor element
//     reaches the ABSENT state, but afterwards in the stage, so as to rules.Not
//     interfere with the event logging op which is tied to the descriptor element
//     removal.
func init() {

	registerDepRule(
		"descriptor TXN_DROPPED before DROPPED",
		scgraph.PreviousStagePrecedence,
		"txn_dropped", "dropped",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				from.El.AttrEqVar(screl.DescID, "_"),
				from.El.AttrEqVar(rel.Self, to.El),
				StatusesToAbsent(from, scpb.Status_TXN_DROPPED, to, scpb.Status_DROPPED),
			}
		})
	registerDepRule(
		"descriptor DROPPED in transaction before removal",
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
		"descriptor drop right before dependent element removal",
		scgraph.SameStagePrecedence,
		"descriptor", "dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.TypeFilter(rulesVersionKey, isSimpleDependent),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
				FromHasPublicStatusIfFromIsTableAndToIsRowLevelTTL(from.Target, from.El, to.El),
			}
		})

	registerDepRule(
		"descriptor removal right before dependent element removal",
		scgraph.SameStagePrecedence,
		"descriptor", "idx-or-col",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.TypeFilter(rulesVersionKey, isSubjectTo2VersionInvariant),
				JoinOnDescID(from, to, "desc-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
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
				to.TypeFilter(rulesVersionKey, isSimpleDependent),
				JoinReferencedDescID(to, from, "desc-id"),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"descriptor drop right before removing dependent with type ref",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-type",
		func(from, to NodeVars) rel.Clauses {
			fromDescID := rel.Var("fromDescID")
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isTypeDescriptor),
				from.JoinTargetNode(),
				from.DescIDEq(fromDescID),
				to.ReferencedTypeDescIDsContain(fromDescID),
				to.TypeFilter(rulesVersionKey, isSimpleDependent, Or(isWithTypeT, isWithExpression)),
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
				from.JoinTargetNode(),
				from.DescIDEq(seqID),
				to.ReferencedSequenceIDsContains(seqID),
				to.TypeFilter(rulesVersionKey, isSimpleDependent, isWithExpression),
				StatusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		},
	)
}
