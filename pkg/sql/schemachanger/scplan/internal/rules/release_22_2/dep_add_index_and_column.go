// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package release_22_2

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules/common"
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
		func(from, to common.NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexColumn)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				common.JoinOnIndexID(from, to, "table-id", "index-id"),
				common.StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_BACKFILLED),
			}
		})

	// We need to make sure that no columns are added to the temp index after it
	// receives any writes.
	registerDepRule("index-column added to index before temp index receives writes",
		scgraph.Precedence,
		"index-column", "index",
		func(from, to common.NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.IndexColumn)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				common.JoinOnIndexID(from, to, "table-id", "index-id"),
				common.StatusesTransient(from, scpb.Status_PUBLIC, to, scpb.Status_WRITE_ONLY),
			}
		})

	registerDepRule(
		"column existence precedes index existence",
		scgraph.Precedence,
		"column", "index",
		func(from, to common.NodeVars) rel.Clauses {
			ic := common.MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.PrimaryIndex)(nil), (*scpb.SecondaryIndex)(nil)),
				common.JoinOnColumnID(from, ic, relationID, columnID),
				common.ColumnInIndex(ic, to, relationID, columnID, "index-id"),
				common.StatusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_BACKFILL_ONLY),
			}
		},
	)

	registerDepRule(
		"column existence precedes temp index existence",
		scgraph.Precedence,
		"column", "index",
		func(from, to common.NodeVars) rel.Clauses {
			ic := common.MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.Column)(nil)),
				to.Type((*scpb.TemporaryIndex)(nil)),
				common.JoinOnColumnID(ic, from, relationID, columnID),
				common.ColumnInIndex(ic, to, relationID, columnID, "index-id"),
				common.StatusesToPublicOrTransient(from, scpb.Status_DELETE_ONLY, to, scpb.Status_DELETE_ONLY),
			}
		},
	)

	// We need to ensure that the temporary index has all the relevant writes
	// to any columns it contains. We ensure elsewhere that any index which
	// will later be merged with the temporary index is common.Not backfilled until
	// that temporary index is receiving writes. This rule ensures that those
	// write operations contain data for all columns.
	registerDepRule(
		"column is WRITE_ONLY before temporary index is WRITE_ONLY",
		scgraph.Precedence,
		"column", "index",
		func(from, to common.NodeVars) rel.Clauses {
			ic := common.MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.El.Type((*scpb.Column)(nil)),
				to.El.Type((*scpb.TemporaryIndex)(nil)),
				common.JoinOnColumnID(ic, from, relationID, columnID),
				common.ColumnInIndex(ic, to, relationID, columnID, "index-id"),
				common.StatusesToPublicOrTransient(from, scpb.Status_WRITE_ONLY, to, scpb.Status_WRITE_ONLY),
			}
		},
	)

	registerDepRule(
		"swapped primary index public before column",
		scgraph.Precedence,
		"index", "column",
		func(from, to common.NodeVars) rel.Clauses {
			ic := common.MkNodeVars("index-column")
			relationID, columnID := rel.Var("table-id"), rel.Var("column-id")
			return rel.Clauses{
				from.Type((*scpb.PrimaryIndex)(nil)),
				to.Type((*scpb.Column)(nil)),
				common.ColumnInSwappedInPrimaryIndex(ic, from, relationID, columnID, "index-id"),
				common.JoinOnColumnID(ic, to, relationID, columnID),
				common.StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)

}
