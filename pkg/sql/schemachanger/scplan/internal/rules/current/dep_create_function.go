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
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

func init() {
	registerDepRule(
		"function existence precedes function dependents",
		scgraph.Precedence,
		"function", "dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.Function)(nil)),
				to.TypeFilter(rulesVersionKey, isFunctionDependent),
				JoinOnDescID(from, to, "function-id"),
				StatusesToPublicOrTransient(from, scpb.Status_DESCRIPTOR_ADDING, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"function dependents exist before function becomes public",
		scgraph.Precedence,
		"dependent", "function",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isFunctionDependent),
				to.Type((*scpb.Function)(nil)),
				JoinOnDescID(from, to, "function-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)

	// When setting object parent ids, we need to add the function to schema, a
	// function name is needed for this.
	registerDepRule(
		"function name should be set before parent ids",
		scgraph.Precedence,
		"function-name", "function-parent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.FunctionName)(nil)),
				to.Type((*scpb.ObjectParent)(nil)),
				JoinOnDescID(from, to, "function-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)

}
