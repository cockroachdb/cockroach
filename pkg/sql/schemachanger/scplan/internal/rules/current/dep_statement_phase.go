// Copyright 2023 The Cockroach Authors.
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
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/opgen"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

func init() {

	findNoopSourceStatuses := func(
		el scpb.Element, targetStatus scpb.TargetStatus,
	) map[scpb.Status][]scpb.Status {
		// We want to skip the dependency edges if the status which got us into
		// the current status was a no-op. We track the no-op status parent nodes,
		// and we'll add a not-join to make sure there does not exist a node
		// with such a status when installing the rule.
		//
		// This is necessary to deal with cases like the transition from
		// BACKFILL_ONLY to its equivalent DELETE_ONLY in the rollback of an
		// ADD COLUMN. We don't want or need a dependency edge from DELETE_ONLY
		// to ABSENT in that case, but if we didn't check whether we got to
		// DELETE_ONLY from BACKFILL_ONLY, then we'd have one implicitly.
		statusMap := map[scpb.Status][]scpb.Status{}
		if err := opgen.IterateTransitions(el, targetStatus, func(
			t opgen.Transition,
		) error {
			if !t.OpType().IsValid() {
				statusMap[t.To()] = append(statusMap[t.To()], t.From())
			}
			return nil
		}); err != nil {
			panic(err)
		}
		return statusMap
	}
	// Ensures that a descriptor is at most allowed a single transition,
	// if it is in an adding state (and the state is revertible).
	clausesForStatementPhaseSingleTransition := func(
		from, to NodeVars,
		el scpb.Element,
		targetStatus scpb.TargetStatus,
		t opgen.Transition,
		prePrevStatuses []scpb.Status,
		isDescriptor bool,
	) rel.Clauses {
		var descIDVar rel.Var = "desc-id"
		clauses := rel.Clauses{
			from.Type(el),
			to.Type(el),
			from.El.AttrEqVar(screl.DescID, descIDVar),
			from.El.AttrEqVar(rel.Self, to.El),
			from.Target.AttrEqVar(rel.Self, to.Target),
			from.Target.AttrEq(screl.TargetStatus, targetStatus.Status()),
			from.Node.AttrEq(screl.CurrentStatus, t.From()),
			to.Node.AttrEq(screl.CurrentStatus, t.To()),
		}
		if !isDescriptor {
			targetDesc := MkNodeVars("target-desc")
			clauses = append(clauses,
				rel.Clauses{
					targetDesc.TypeFilter(rulesVersionKey, IsDescriptor),
					targetDesc.JoinTargetNode(),
					targetDesc.DescIDEq(descIDVar),
					targetDesc.Target.AttrNeq(screl.CurrentStatus, scpb.Status_DESCRIPTOR_ADDED),
				}...)
		} else if t.Revertible() {
			clauses = append(clauses,
				from.Target.AttrNeq(screl.CurrentStatus, scpb.Status_DESCRIPTOR_ADDED))
		}
		if len(prePrevStatuses) > 0 {
			clauses = append(clauses,
				GetNotJoinOnNodeWithStatusIn(prePrevStatuses)(from.Target),
			)
		}
		return clauses
	}

	addRules := func(el scpb.Element, targetStatus scpb.TargetStatus, isDescriptor bool) {
		statusMap := findNoopSourceStatuses(el, targetStatus)
		fromPublicStatus := scpb.Status_UNKNOWN
		fromAbsentStatus := scpb.Status_UNKNOWN

		if err := opgen.IterateTransitions(el, targetStatus, func(
			t opgen.Transition,
		) error {
			if targetStatus == scpb.ToPublic && t.From() == scpb.Status_ABSENT {
				fromPublicStatus = t.To()
				return nil
			}
			if targetStatus == scpb.ToAbsent && t.From() == scpb.Status_PUBLIC {
				fromAbsentStatus = t.To()
				return nil
			}
			if ((t.From() != fromPublicStatus) && targetStatus == scpb.ToPublic) ||
				((t.From() != fromAbsentStatus) && targetStatus == scpb.ToAbsent) {
				return nil
			}
			// This transition is already covered.
			if t.From() == scpb.Status_DROPPED && t.To() == scpb.Status_ABSENT {
				return nil
			}
			elemName := reflect.TypeOf(el).Elem().Name()
			ruleName := scgraph.RuleName(fmt.Sprintf(
				"%s transitions to %s uphold only single transition in statement phase: %s->%s",
				elemName, targetStatus.Status(), t.From(), t.To(),
			))
			registerDepRule(
				ruleName,
				scgraph.StatementPhasePrecedence,
				"prev", "next",
				func(from, to NodeVars) rel.Clauses {
					return clausesForStatementPhaseSingleTransition(
						from, to, el, targetStatus, t, statusMap[t.From()],
						isDescriptor,
					)
				},
			)
			return nil
		}); err != nil {
			panic(err)
		}
	}

	_ = scpb.ForEachElementType(func(el scpb.Element) error {
		isDescriptor := IsDescriptor(el)
		if opgen.HasPublic(el) {
			addRules(el, scpb.ToPublic, isDescriptor)
		}
		addRules(el, scpb.ToAbsent, isDescriptor) // every element has ToAbsent
		return nil
	})
}
