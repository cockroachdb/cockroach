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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

var notJoinOnNodeWithStatusIn = map[string]rel.Rule1{}

func makeStatusesString(statuses []scpb.Status) string {
	return strings.Join(makeStatusesStrings(statuses), "_")
}

func makeStatusesStrings(statuses []scpb.Status) []string {
	ret := make([]string, len(statuses))
	for i, status := range statuses {
		ret[i] = status.String()
	}
	return ret
}

func getNotJoinOnNodeWithStatusIn(statues []scpb.Status) rel.Rule1 {
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

// These rules ensure that changes to properties of descriptors which need to
// be sequenced in order to safely enact online schema changes are sequenced
// in separate transactions.
func init() {
	type depElement struct {
		name string
		el   scpb.Element
	}
	type statusFunc = func(
		from nodeVars, fromStatus scpb.Status, to nodeVars, toStatus scpb.Status,
	) rel.Clause
	addRules := func(c depElement, targetStatus scpb.TargetStatus, sf statusFunc) {
		// We want to skip the dependency edges if the status which got us into
		// the current status was a no-op. We track the no-op status parent nodes,
		// and we'll add a not-join to make sure there does not exist a node
		// with such a status when installing the rule.
		//
		// This code is necessary to deal with cases like the transition from
		// BACKFILL_ONLY to its equivalent DELETE_ONLY in the rollback of an
		// ADD COLUMN. We don't want or need a dependency edge from DELETE_ONLY
		// to ABSENT in that case, but if we didn't check whether we got to
		// DELETE_ONLY from BACKFILL_ONLY, then we'd have one implicitly.
		statusMap := map[scpb.Status][]scpb.Status{}
		if err := opgen.IterateTransitions(c.el, targetStatus, func(t opgen.Transition) error {
			if !t.OpType().IsValid() {
				statusMap[t.To()] = append(statusMap[t.To()], t.From())
			}
			return nil
		}); err != nil {
			panic(err)
		}
		if err := opgen.IterateTransitions(c.el, targetStatus, func(t opgen.Transition) error {

			prePrevStatuses := statusMap[t.From()]
			registerDepRule(
				scgraph.RuleName(fmt.Sprintf(
					"%s transitions to %s uphold 2-version invariant: %s->%s",
					c.name, targetStatus.Status(), t.From(), t.To(),
				)),
				scgraph.PreviousTransactionPrecedence,
				"prev", "next",
				func(from, to nodeVars) rel.Clauses {
					clauses := rel.Clauses{
						from.Type(c.el),
						to.Type(c.el),
						from.el.AttrEqVar(screl.DescID, "_"),
						from.el.AttrEqVar(rel.Self, to.el),
						from.target.AttrEqVar(rel.Self, to.target),
						from.target.AttrEq(screl.TargetStatus, targetStatus.Status()),
						from.node.AttrEq(screl.CurrentStatus, t.From()),
						to.node.AttrEq(screl.CurrentStatus, t.To()),
						descriptorIsNotBeingDropped(from.el),
					}
					if len(prePrevStatuses) > 0 {
						clauses = append(clauses,
							getNotJoinOnNodeWithStatusIn(prePrevStatuses)(from.target))
					}
					return clauses
				})
			return nil
		}); err != nil {
			panic(err)
		}
	}
	for _, c := range []depElement{
		{
			name: "column",
			el:   (*scpb.Column)(nil),
		},
		{
			name: "primary index",
			el:   (*scpb.PrimaryIndex)(nil),
		},
		{
			name: "secondary index",
			el:   (*scpb.SecondaryIndex)(nil),
		},
		{
			name: "temporary index",
			el:   (*scpb.TemporaryIndex)(nil),
		},
		{
			name: "foreign key",
			el:   (*scpb.ForeignKeyConstraint)(nil),
		},
		{
			name: "check constraint",
			el:   (*scpb.CheckConstraint)(nil),
		},
		{
			name: "unique without index constraint",
			el:   (*scpb.UniqueWithoutIndexConstraint)(nil),
		},
		{
			name: "enum value",
			el:   (*scpb.EnumTypeValue)(nil),
		},
	} {
		if opgen.HasPublic(c.el) {
			addRules(c, scpb.ToPublic, statusesToPublicOrTransient)
		}
		if opgen.HasTransient(c.el) {
			addRules(c, scpb.Transient, statusesToPublicOrTransient)
		}
		addRules(c, scpb.ToAbsent, statusesToAbsent) // every element has ToAbsent
	}
}

func boxStatuses(input []scpb.Status) []interface{} {
	ret := make([]interface{}, len(input))
	for i, s := range input {
		ret[i] = s
	}
	return ret
}
