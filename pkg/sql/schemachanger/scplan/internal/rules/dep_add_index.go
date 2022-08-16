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

// These rules ensure that index-dependent elements, like an index's name, its
// partitioning, etc. appear once the index reaches a suitable state.
func init() {

	registerDepRule(
		"index existence precedes index dependents",
		scgraph.Precedence,
		"index", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				to.typeFilter(isIndexDependent),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToPublicOrTransient(from, scpb.Status_BACKFILL_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"temp index existence precedes index dependents",
		scgraph.Precedence,
		"index", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.typeFilter(isIndexDependent),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"index dependents exist before index becomes public",
		scgraph.Precedence,
		"dependent", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.typeFilter(isIndexDependent),
				to.typeFilter(isIndex),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}

// Special cases of the above.
func init() {

	registerDepRule(
		"index named right before index becomes public",
		scgraph.SameStagePrecedence,
		"index-name", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexName)(nil)),
				to.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				joinOnIndexID(from, to, "table-id", "index-id"),
				statusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}

// This rule ensures that before an offline-backfilled index can begin
// backfilling, the corresponding temporary index exists in WRITE_ONLY.
func init() {

	registerDepRule(
		"temp index is WRITE_ONLY before backfill",
		scgraph.Precedence,
		"temp", "index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.TemporaryIndexID,
					"temp-index-id",
				),
				from.targetStatus(scpb.Transient),
				to.targetStatus(scpb.ToPublic, scpb.Transient),
				from.currentStatus(scpb.Status_WRITE_ONLY),
				to.currentStatus(scpb.Status_BACKFILLED),
			}
		},
	)
}

// We want to say that all columns which are part of a secondary index need
// to be in a primary index which is validated
// To do that, we want to find a secondary index which has a source which
// is a primary index which is itself new.
func init() {

	registerDepRule(
		"primary index with new columns should exist before secondary indexes",
		scgraph.Precedence,
		"primary-index", "secondary-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.SecondaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"primary-index-id",
				),
				statusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_BACKFILL_ONLY),
			}
		})

	registerDepRule(
		"primary index with new columns should exist before temp indexes",
		scgraph.Precedence,
		"primary-index", "temp-index",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				joinOnDescID(from, to, "table-id"),
				joinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"primary-index-id",
				),
				statusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_DELETE_ONLY),
			}
		})
}
