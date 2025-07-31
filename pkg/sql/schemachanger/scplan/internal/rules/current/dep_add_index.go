// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package current

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
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
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				to.TypeFilter(rulesVersionKey, isIndexDependent),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_BACKFILL_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"temp index existence precedes index dependents",
		scgraph.Precedence,
		"index", "dependent",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.TypeFilter(rulesVersionKey, isIndexDependent),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"index dependents exist before index becomes public",
		scgraph.Precedence,
		"dependent", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isIndexDependent),
				to.TypeFilter(rulesVersionKey, isIndex),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}

// Special cases of the above.
func init() {

	registerDepRule(
		"primary index named right before index becomes public",
		scgraph.SameStagePrecedence,
		"index-name", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexName)(nil)),
				to.Type(
					(*scpb.PrimaryIndex)(nil),
				),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
	registerDepRule(
		"secondary index named before public (with index swap)",
		scgraph.Precedence,
		"index", "index-name",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				to.Type((*scpb.IndexName)(nil)),
				from.Type(
					(*scpb.SecondaryIndex)(nil),
				),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_VALIDATED, to, scpb.Status_PUBLIC),
				rel.And(IsPotentialSecondaryIndexSwap("index-id", "table-id")...),
			}
		},
	)

	registerDepRule(
		"secondary index named before validation (without index swap)",
		scgraph.Precedence,
		"index-name", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexName)(nil)),
				to.Type(
					(*scpb.SecondaryIndex)(nil),
				),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				IsNotPotentialSecondaryIndexSwap("table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_VALIDATED),
			}
		},
	)
}

// This rule ensures that primary indexes and their corresponding temporary
// indexes appear in an appropriate order to correctly support index backfilling.
func init() {

	// Offline-backfilled index can begin backfilling after the corresponding
	// temporary index exists in WRITE_ONLY.
	registerDepRule(
		"temp index is WRITE_ONLY before backfill",
		scgraph.Precedence,
		"temp", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.IndexID,
					to, screl.TemporaryIndexID,
					"temp-index-id",
				),
				from.TargetStatus(scpb.Transient),
				to.TargetStatus(scpb.ToPublic, scpb.Transient),
				from.CurrentStatus(scpb.Status_WRITE_ONLY),
				to.CurrentStatus(scpb.Status_BACKFILLED),
			}
		},
	)

	// The following two rules together ensure that temporary index is dropped
	// after its master index has merged its data (MERGED) and before its master
	// index advances into the next status (WRITE_ONLY).

	// Temporary index starts to disappear after its master index has merged
	// this temporary index's data.
	registerDepRule(
		"index is MERGED before its temp index starts to disappear",
		scgraph.Precedence,
		"index", "temp",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.TemporaryIndexID,
					to, screl.IndexID,
					"temp-index-id",
				),
				from.TargetStatus(scpb.ToPublic, scpb.Transient),
				from.CurrentStatus(scpb.Status_MERGED),
				to.TargetStatus(scpb.Transient),
				to.CurrentStatus(scpb.Status_TRANSIENT_DELETE_ONLY),
			}
		},
	)

	// Temporary index disappeared before its master index reaches WRITE_ONLY.
	registerDepRule(
		"temp index disappeared before its master index reaches WRITE_ONLY",
		scgraph.Precedence,
		"temp", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.IndexID,
					to, screl.TemporaryIndexID,
					"temp-index-id",
				),
				from.TargetStatus(scpb.Transient),
				from.CurrentStatus(scpb.Status_TRANSIENT_DELETE_ONLY),
				to.TargetStatus(scpb.ToPublic, scpb.Transient),
				to.CurrentStatus(scpb.Status_WRITE_ONLY),
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
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.SecondaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"primary-index-id",
				),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_BACKFILL_ONLY),
			}
		})

	registerDepRule(
		"primary index with new columns should exist before temp indexes",
		scgraph.Precedence,
		"primary-index", "temp-index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				JoinOnDescID(from, to, "table-id"),
				JoinOn(
					from, screl.IndexID,
					to, screl.SourceIndexID,
					"primary-index-id",
				),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_DELETE_ONLY),
			}
		})
}
