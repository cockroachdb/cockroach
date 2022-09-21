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
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/opgen"
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
		from, to nodeVars,
		el scpb.Element,
		targetStatus scpb.TargetStatus,
		t opgen.Transition,
		prePrevStatuses []scpb.Status,
	) rel.Clauses {
		clauses := rel.Clauses{
			from.Type(el),
			to.Type(el),
			from.el.AttrEqVar(screl.DescID, "_"),
			from.el.AttrEqVar(rel.Self, to.el),
			from.target.AttrEqVar(rel.Self, to.target),
			from.target.AttrEq(screl.TargetStatus, targetStatus.Status()),
			from.node.AttrEq(screl.CurrentStatus, t.From()),
			to.node.AttrEq(screl.CurrentStatus, t.To()),
			from.descriptorIsNotBeingDropped(),
		}
		if len(prePrevStatuses) > 0 {
			clauses = append(clauses,
				getNotJoinOnNodeWithStatusIn(prePrevStatuses)(from.target),
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
				func(from, to nodeVars) rel.Clauses {
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
	_ = forEachElement(func(el scpb.Element) error {
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

// notJoinOnNodeWithStatusIn is a cache to memoize getNotJoinOnNodeWithStatusIn.
var notJoinOnNodeWithStatusIn = map[string]rel.Rule1{}

// getNoJoinOnNodeWithStatusIn returns a not-join rule which takes a variable
// corresponding to a target in the graph as input and will exclude that target
// if the graph contains a node with that target in any of the listed status
// values.
func getNotJoinOnNodeWithStatusIn(statues []scpb.Status) rel.Rule1 {
	makeStatusesStrings := func(statuses []scpb.Status) []string {
		ret := make([]string, len(statuses))
		for i, status := range statuses {
			ret[i] = status.String()
		}
		return ret
	}
	makeStatusesString := func(statuses []scpb.Status) string {
		return strings.Join(makeStatusesStrings(statuses), "_")
	}
	boxStatuses := func(input []scpb.Status) []interface{} {
		ret := make([]interface{}, len(input))
		for i, s := range input {
			ret[i] = s
		}
		return ret
	}
	name := makeStatusesString(statues)
	if got, ok := notJoinOnNodeWithStatusIn[name]; ok {
		return got
	}
	r := screl.Schema.DefNotJoin1(
		fmt.Sprintf("nodeNotExistsWithStatusIn_%s", name),
		"sharedTarget", func(target rel.Var) rel.Clauses {
			n := rel.Var("n")
			return rel.Clauses{
				n.Type((*screl.Node)(nil)),
				n.AttrEqVar(screl.Target, target),
				n.AttrIn(screl.CurrentStatus, boxStatuses(statues)...),
			}
		})
	notJoinOnNodeWithStatusIn[name] = r
	return r
}
