// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package current

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/common"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

// These rules ensure that constraint-dependent elements, like an constraint's
// name, etc. disappear once the constraint reaches a suitable state.
// TODO (xiang): The dep rules here are not complete, as they are aimed specifically
// for check constraints only. Complete them when we properly support the
// other constraints: UniqueWithoutIndex, ForeignKey, Unique, and Not Null.
func init() {

	registerDepRuleForDrop(
		"constraint no longer public before dependents",
		scgraph.Precedence,
		"constraint", "dependent",
		scpb.Status_VALIDATED, scpb.Status_ABSENT,
		func(from, to common.NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(common.IsSupportedNonIndexBackedConstraint),
				to.TypeFilter(common.IsConstraintDependent),
				common.JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)

	registerDepRuleForDrop(
		"dependents removed before constraint",
		scgraph.Precedence,
		"dependents", "constraint",
		scpb.Status_ABSENT, scpb.Status_ABSENT,
		func(from, to common.NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(common.IsConstraintDependent),
				to.TypeFilter(common.IsSupportedNonIndexBackedConstraint),
				common.JoinOnConstraintID(from, to, "table-id", "constraint-id"),
			}
		},
	)
}
