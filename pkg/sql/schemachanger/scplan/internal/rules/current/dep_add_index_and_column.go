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
)

// These rules ensure that columns and indexes containing these columns
// appear into existence in the correct order.
func init() {

	// We need to make sure that no columns are added to the index after it
	// receives any data due to a backfill.
	registerDepRule("index-column added to index before index is backfilled",
		scgraph.Precedence,
		"index-column", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexColumn)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_BACKFILLED),
			}
		})

	// We need to make sure that no columns are added to the temp index after it
	// receives any writes.
	registerDepRule("index-column added to index before temp index receives writes",
		scgraph.Precedence,
		"index-column", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexColumn)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesTransient(from, scpb.Status_PUBLIC, to, scpb.Status_WRITE_ONLY),
			}
		})

	registerDepRule(
		"column existence precedes index existence",
		scgraph.Precedence,
		"column", "index",
		func(from, to NodeVars) rel.Clauses {
			ic := MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				JoinOnColumnID(from, ic, relationID, columnID),
				ColumnInIndex(ic, to, relationID, columnID, "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_BACKFILL_ONLY),
			}
		},
	)

	registerDepRule(
		"column existence precedes temp index existence",
		scgraph.Precedence,
		"column", "index",
		func(from, to NodeVars) rel.Clauses {
			ic := MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				JoinOnColumnID(ic, from, relationID, columnID),
				ColumnInIndex(ic, to, relationID, columnID, "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_DELETE_ONLY),
			}
		},
	)

	// We need to ensure that the temporary index has all the relevant writes
	// to any columns it contains. We ensure elsewhere that any index which
	// will later be merged with the temporary index is not backfilled until
	// that temporary index is receiving writes. This rule ensures that those
	// write operations contain data for all columns.
	registerDepRule(
		"column is WRITE_ONLY before temporary index is WRITE_ONLY",
		scgraph.Precedence,
		"column", "index",
		func(from, to NodeVars) rel.Clauses {
			ic := MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.El.Type((*scpb.Column)(nil)),
				to.El.Type((*scpb.TemporaryIndex)(nil)),
				JoinOnColumnID(ic, from, relationID, columnID),
				ColumnInIndex(ic, to, relationID, columnID, "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_WRITE_ONLY, to, scpb.Status_WRITE_ONLY),
			}
		},
	)

	registerDepRule(
		"swapped primary index public before column",
		scgraph.Precedence,
		"index", "column",
		func(from, to NodeVars) rel.Clauses {
			ic := MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				ColumnInSwappedInPrimaryIndex(ic, from, relationID, columnID, "index-id"),
				JoinOnColumnID(ic, to, relationID, columnID),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)

}
