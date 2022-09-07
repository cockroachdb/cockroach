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

// These rules ensure that:
//   - a descriptor reaches the TXN_DROPPED state in the statement phase, and
//     it does not reach DROPPED until the pre-commit phase.
//   - a descriptor reaches ABSENT in a different transaction than it reaches
//     DROPPED (i.e. it cannot be removed until PostCommit).
//   - a descriptor element reaches the DROPPED state in the txn before
//     its dependent elements (namespace entry, comments, column names, etc) reach
//     the ABSENT state;
//   - for those dependent elements which have to wait post-commit to reach the
//     ABSENT state, we tie them to the same stage as when the descriptor element
//     reaches the ABSENT state, but afterwards in the stage, so as to not
//     interfere with the event logging op which is tied to the descriptor element
//     removal.
func init() {

	registerDepRule(
		"descriptor TXN_DROPPED before DROPPED",
		scgraph.PreviousStagePrecedence,
		"txn_dropped", "dropped",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(IsDescriptor),
				from.el.AttrEqVar(screl.DescID, "_"),
				from.el.AttrEqVar(rel.Self, to.el),
				statusesToAbsent(from, scpb.Status_TXN_DROPPED, to, scpb.Status_DROPPED),
			}
		})
	registerDepRule(
		"descriptor DROPPED in transaction before removal",
		scgraph.PreviousTransactionPrecedence,
		"dropped", "absent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(IsDescriptor),
				from.el.AttrEqVar(screl.DescID, "_"),
				from.el.AttrEqVar(rel.Self, to.el),
				statusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		})

	registerDepRule(
		"descriptor drop right before dependent element removal",
		scgraph.SameStagePrecedence,
		"descriptor", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(IsDescriptor),
				to.typeFilter(isSimpleDependent),
				joinOnDescID(from, to, "desc-id"),
				statusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		})

	registerDepRule(
		"descriptor removal right before dependent element removal",
		scgraph.SameStagePrecedence,
		"descriptor", "idx-or-col",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(IsDescriptor),
				to.typeFilter(isSubjectTo2VersionInvariant),
				joinOnDescID(from, to, "desc-id"),
				statusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_ABSENT),
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
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(IsDescriptor),
				to.typeFilter(isSimpleDependent),
				joinReferencedDescID(to, from, "desc-id"),
				statusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
			}
		},
	)

	registerDepRule(
		"descriptor drop right before removing dependent with type ref",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-type",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(IsDescriptor),
				to.typeFilter(isSimpleDependent, isWithTypeT),
				statusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
				filterElements("RefByTypeT", from, to, func(from, to scpb.Element) bool {
					refID := screl.GetDescID(from)
					typeT := getTypeTOrPanic(to)
					return typeT != nil && idInIDs(typeT.ClosedTypeIDs, refID)
				}),
			}
		},
	)

	registerDepRule(
		"descriptor drop right before removing dependent with expr ref",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-expr",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(IsDescriptor),
				to.typeFilter(isSimpleDependent, isWithExpression),
				statusesToAbsent(from, scpb.Status_DROPPED, to, scpb.Status_ABSENT),
				filterElements("RefByExpression", from, to, func(from, to scpb.Element) bool {
					refID := screl.GetDescID(from)
					expr := getExpressionOrPanic(to)
					return expr != nil && (idInIDs(expr.UsesTypeIDs, refID) || idInIDs(expr.UsesSequenceIDs, refID))
				}),
			}
		},
	)
}
