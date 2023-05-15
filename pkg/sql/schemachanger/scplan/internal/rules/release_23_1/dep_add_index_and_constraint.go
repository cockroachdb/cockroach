// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package release_23_1

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// These rules ensure that indexes and constraints on those indexes come
// to existence in the appropriate order.
func init() {
	registerDepRule(
		"index is ready to be validated before we validate constraint on it",
		scgraph.Precedence,
		"index", "constraint",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.TypeFilter(rulesVersionKey, isNonIndexBackedConstraint, isSubjectTo2VersionInvariant),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.IndexID,
					to, screl.IndexID,
					"index-id-for-validation",
				),
				StatusesToPublicOrTransient(from, scpb.Status_VALIDATED, to, scpb.Status_VALIDATED),
			}
		},
	)
}
