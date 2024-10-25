// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_24_3

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

// These rules ensure that changes to properties of descriptors which need to
// be sequenced in order to safely enact online schema changes are sequenced
// in separate transactions.
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
	clausesForTwoVersionEdge := func(
		from, to NodeVars,
		el scpb.Element,
		targetStatus scpb.TargetStatus,
		t opgen.Transition,
		prePrevStatuses []scpb.Status,
	) rel.Clauses {
		descriptorData := MkNodeVars("descriptor-data")
		var descID rel.Var = "descID"
		clauses := rel.Clauses{
			from.Type(el),
			to.Type(el),
			from.El.AttrEqVar(screl.DescID, descID),
			from.El.AttrEqVar(rel.Self, to.El),
			from.Target.AttrEqVar(rel.Self, to.Target),
			from.Target.AttrEq(screl.TargetStatus, targetStatus.Status()),
			from.Node.AttrEq(screl.CurrentStatus, t.From()),
			to.Node.AttrEq(screl.CurrentStatus, t.To()),
			descriptorIsNotBeingDropped(from.El),
			// Make sure to join a data element to confirm that data exists.
			descriptorData.Type((*scpb.TableData)(nil)),
			descriptorData.JoinTargetNode(),
			descriptorData.CurrentStatus(scpb.Status_PUBLIC),
			descriptorData.DescIDEq(descID),
			descriptorDataIsNotBeingAdded(descID),
		}
		if len(prePrevStatuses) > 0 {
			clauses = append(clauses,
				GetNotJoinOnNodeWithStatusIn(prePrevStatuses)(from.Target),
			)
		}
		return clauses
	}
	addRules := func(el scpb.Element, targetStatus scpb.TargetStatus) {
		statusMap := findNoopSourceStatuses(el, targetStatus)
		if err := opgen.IterateTransitions(el, targetStatus, func(
			t opgen.Transition,
		) error {
			elemName := reflect.TypeOf(el).Elem().Name()
			ruleName := scgraph.RuleName(fmt.Sprintf(
				"%s transitions to %s uphold 2-version invariant: %s->%s",
				elemName, targetStatus.Status(), t.From(), t.To(),
			))
			registerDepRule(
				ruleName,
				scgraph.PreviousTransactionPrecedence,
				"prev", "next",
				func(from, to NodeVars) rel.Clauses {
					return clausesForTwoVersionEdge(
						from, to, el, targetStatus, t, statusMap[t.From()],
					)
				},
			)
			return nil
		}); err != nil {
			panic(err)
		}
	}
	_ = scpb.ForEachElementType(func(el scpb.Element) error {
		if !isSubjectTo2VersionInvariant(el) {
			return nil
		}
		if opgen.HasPublic(el) {
			addRules(el, scpb.ToPublic)
		}
		if opgen.HasTransient(el) {
			addRules(el, scpb.Transient)
		}
		addRules(el, scpb.ToAbsent) // every element has ToAbsent
		return nil
	})
}
