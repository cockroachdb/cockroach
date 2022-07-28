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
// - a descriptor element reaches the DROPPED state in the statement txn before
//   its dependent elements (namespace entry, comments, column names, etc) reach
//   the ABSENT state;
// - for those dependent elements which have to wait post-commit to reach the
//   ABSENT state, we tie them to the same stage as when the descriptor element
//   reaches the ABSENT state, but afterwards in the stage, so as to not
//   interfere with the event logging op which is tied to the descriptor element
//   removal.
func init() {

	registerDepRule(
		"descriptor drop right before dependent element removal",
		scgraph.SameStagePrecedence,
		"descriptor", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				elementTypes(from, IsDescriptor),
				elementTypes(to, isSimpleDependent),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DROPPED),
				currentStatus(to.node, scpb.Status_ABSENT),
				joinOnDescID(from.el, to.el, "desc-id"),
			}
		})

	registerDepRule(
		"descriptor removal right before dependent element removal",
		scgraph.SameStagePrecedence,
		"descriptor", "idx-or-col",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				elementTypes(from, IsDescriptor),
				elementTypes(to, isSubjectTo2VersionInvariant),
				toAbsentInAbsent(from.target, from.node, to.target, to.node),
				joinOnDescID(from.el, to.el, "desc-id"),
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
				elementTypes(from, IsDescriptor),
				elementTypes(to, isSimpleDependent),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DROPPED),
				currentStatus(to.node, scpb.Status_ABSENT),
				joinReferencedDescID(to.el, from.el, "desc-id"),
			}
		},
	)

	registerDepRule(
		"descriptor drop right before removing dependent with type ref",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-type",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				elementTypes(from, IsDescriptor),
				elementTypes(to, isSimpleDependent, isWithTypeT),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DROPPED),
				currentStatus(to.node, scpb.Status_ABSENT),
				rel.Filter("RefByTypeT", from.el, to.el)(func(ref, e scpb.Element) bool {
					refID := screl.GetDescID(ref)
					typeT := getTypeTOrPanic(e)
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
				elementTypes(from, IsDescriptor),
				elementTypes(to, isSimpleDependent, isWithExpression),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DROPPED),
				currentStatus(to.node, scpb.Status_ABSENT),
				rel.Filter("RefByTypeT", from.el, to.el)(func(ref, e scpb.Element) bool {
					refID := screl.GetDescID(ref)
					expr := getExpressionOrPanic(e)
					return expr != nil && (idInIDs(expr.UsesTypeIDs, refID) || idInIDs(expr.UsesSequenceIDs, refID))
				}),
			}
		},
	)
}
