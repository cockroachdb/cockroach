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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

// Rules related to garbage collection.
// Garbage collection must occur:
// - in the same stage as the descriptor disappears;
// - for indexes, not before the index disappears;
// - all in the same stage for each descriptor.
func init() {

	registerDepRule(
		"table removed right before garbage collection",
		scgraph.SameStagePrecedence,
		"table", "data",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.Type((*scpb.TableData)(nil)),
				JoinOnDescID(from, to, "table-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_DROPPED),
			}
		},
	)

	registerDepRule(
		"descriptor removed right before garbage collection",
		scgraph.SameStagePrecedence,
		"database", "data",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.Type((*scpb.DatabaseData)(nil)),
				JoinOnDescID(from, to, "db-id"),
				StatusesToAbsent(from, scpb.Status_ABSENT, to, scpb.Status_DROPPED),
			}
		},
	)

	registerDepRuleForDrop(
		"index removed before garbage collection",
		scgraph.Precedence,
		"index", "index-data",
		scpb.Status_ABSENT, scpb.Status_DROPPED,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isIndex),
				to.Type((*scpb.IndexData)(nil)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
			}
		},
	)

	dataIDs := func(data scpb.Element) (catid.DescID, catid.IndexID) {
		switch data := data.(type) {
		case *scpb.DatabaseData:
			return data.DatabaseID, 0
		case *scpb.TableData:
			return data.TableID, 0
		case *scpb.IndexData:
			return data.TableID, data.IndexID
		}
		return 0, 0
	}

	// GC jobs should all be scheduled in the same transaction.
	registerDepRuleForDrop(
		"schedule all GC jobs for a descriptor in the same stage",
		scgraph.SameStagePrecedence,
		"data-a", "data-b",
		scpb.Status_DROPPED, scpb.Status_DROPPED,
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isData),
				to.TypeFilter(rulesVersionKey, isData),
				JoinOnDescID(from, to, "desc-id"),
				FilterElements("SmallerIDsFirst", from, to, func(a, b scpb.Element) bool {
					aDescID, aIdxID := dataIDs(a)
					bDescID, bIdxID := dataIDs(b)
					if aDescID == bDescID {
						return aIdxID < bIdxID
					}
					return aDescID < bDescID
				}),
			}
		},
	)
}

// Rules to ensure proper garbage collection on rollbacks.
// A GC job is required as soon as a new index receives data.
func init() {

	registerDepRule(
		"index data exists as soon as index accepts backfills",
		scgraph.SameStagePrecedence,
		"index-name", "index",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type(
					(*scpb.PrimaryIndex)(nil),
					(*scpb.SecondaryIndex)(nil),
				),
				to.Type((*scpb.IndexData)(nil)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_BACKFILL_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)

	registerDepRule(
		"temp index data exists as soon as temp index accepts writes",
		scgraph.SameStagePrecedence,
		"temp-index", "temp-index-data",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TemporaryIndex)(nil)),
				to.Type((*scpb.IndexData)(nil)),
				JoinOnIndexID(from, to, "table-id", "index-id"),
				StatusesToPublicOrTransient(from, scpb.Status_WRITE_ONLY, to, scpb.Status_PUBLIC),
			}
		},
	)
}

// Rules to ensure for created objects the table data will be live after the
// descriptor is public.
func init() {
	registerDepRule(
		"table added right before data element",
		scgraph.Precedence,
		"table", "data",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isDescriptor),
				to.TypeFilter(rulesVersionKey, isData),
				JoinOnDescID(from, to, "table-id"),
				StatusesToPublicOrTransient(from, scpb.Status_PUBLIC, to, scpb.Status_PUBLIC),
			}
		},
	)
}
