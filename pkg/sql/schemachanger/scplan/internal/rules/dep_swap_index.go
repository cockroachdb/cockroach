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

// This rule ensures that a new primary index becomes public right after the
// old primary index starts getting removed, effectively swapping one for the
// other. This rule also applies when the schema change gets reverted.
func init() {

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"old-index", "new-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"old-index-id",
				),
				from.targetStatus(scpb.ToAbsent),
				from.currentStatus(scpb.Status_VALIDATED),
				to.targetStatus(scpb.ToPublic, scpb.Transient),
				to.currentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"old-index", "new-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"old-index-id",
				),
				from.targetStatus(scpb.Transient),
				from.currentStatus(scpb.Status_TRANSIENT_VALIDATED),
				to.targetStatus(scpb.ToPublic, scpb.Transient),
				to.currentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"primary index swap",
		scgraph.SameStagePrecedence,
		"new-index", "old-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.SourceIndexID,
					to, screl.IndexID,
					"old-index-id",
				),
				from.targetStatus(scpb.ToAbsent),
				from.currentStatus(scpb.Status_VALIDATED),
				to.targetStatus(scpb.ToPublic),
				to.currentStatus(scpb.Status_PUBLIC),
			}
		},
	)
}
