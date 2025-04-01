// Copyright 2025 The Cockroach Authors.
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

// The following set of rules ensure that zone configs depend on each other in
// increasing seqNum order.

func init() {
	// Table zone configs
	registerDepRule(
		"ensure table zone configs are in increasing seqNum order",
		scgraph.Precedence,
		"later-seqNum", "earlier-seqNum",
		func(from, to NodeVars) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.Type((*scpb.TableZoneConfig)(nil)),
				from.JoinTargetNode(),
				to.Type((*scpb.TableZoneConfig)(nil)),
				// Join on the target ID to ensure we're only comparing zone configs
				// for the same object (database, table, index, etc.)
				JoinOnDescID(from, to, "seqnum"),
				ToPublicOrTransient(from, to),
				status.Entities(screl.CurrentStatus, from.Node, to.Node),
				FilterElements("SmallerSeqNumFirst", from, to, func(from, to *scpb.TableZoneConfig) bool {
					return from.SeqNum < to.SeqNum
				}),
			}
		})

	// Database zone configs
	registerDepRule(
		"ensure database zone configs are in increasing seqNum order",
		scgraph.Precedence,
		"later-seqNum", "earlier-seqNum",
		func(from, to NodeVars) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.Type((*scpb.DatabaseZoneConfig)(nil)),
				from.JoinTargetNode(),
				to.Type((*scpb.DatabaseZoneConfig)(nil)),
				JoinOnDescID(from, to, "seqnum"),
				ToPublicOrTransient(from, to),
				status.Entities(screl.CurrentStatus, from.Node, to.Node),
				FilterElements("SmallerSeqNumFirst", from, to, func(from, to *scpb.DatabaseZoneConfig) bool {
					return from.SeqNum < to.SeqNum
				}),
			}
		})

	// Named range zone configs
	registerDepRule(
		"ensure named range zone configs are in increasing seqNum order",
		scgraph.Precedence,
		"later-seqNum", "earlier-seqNum",
		func(from, to NodeVars) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.Type((*scpb.NamedRangeZoneConfig)(nil)),
				from.JoinTargetNode(),
				to.Type((*scpb.NamedRangeZoneConfig)(nil)),
				JoinOnDescID(from, to, "seqnum"),
				ToPublicOrTransient(from, to),
				status.Entities(screl.CurrentStatus, from.Node, to.Node),
				FilterElements("SmallerSeqNumFirst", from, to, func(from, to *scpb.NamedRangeZoneConfig) bool {
					return from.SeqNum < to.SeqNum
				}),
			}
		})

	// Index zone configs
	registerDepRule(
		"ensure index zone configs are in increasing seqNum order",
		scgraph.Precedence,
		"later-seqNum", "earlier-seqNum",
		func(from, to NodeVars) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.Type((*scpb.IndexZoneConfig)(nil)),
				from.JoinTargetNode(),
				to.Type((*scpb.IndexZoneConfig)(nil)),
				JoinOnDescID(from, to, "seqnum"),
				// Join on index ID to ensure we're only comparing zone configs
				// for the same index
				JoinOnIndexID(from, to, "table-id", "index-id"),
				ToPublicOrTransient(from, to),
				status.Entities(screl.CurrentStatus, from.Node, to.Node),
				FilterElements("SmallerSeqNumFirst", from, to, func(from, to *scpb.IndexZoneConfig) bool {
					return from.SeqNum < to.SeqNum
				}),
			}
		})

	// Partition zone configs
	registerDepRule(
		"ensure partition zone configs are in increasing seqNum order",
		scgraph.Precedence,
		"later-seqNum", "earlier-seqNum",
		func(from, to NodeVars) rel.Clauses {
			status := rel.Var("status")
			return rel.Clauses{
				from.Type((*scpb.PartitionZoneConfig)(nil)),
				from.JoinTargetNode(),
				to.Type((*scpb.PartitionZoneConfig)(nil)),
				JoinOnDescID(from, to, "seqnum"),
				// Join on index ID and partition name to ensure we're only comparing
				// zone configs for the same partition
				JoinOnIndexID(from, to, "table-id", "index-id"),
				JoinOnPartitionName(from, to, "table-id", "index-id", "partition-name"),
				ToPublicOrTransient(from, to),
				status.Entities(screl.CurrentStatus, from.Node, to.Node),
				FilterElements("SmallerSeqNumFirst", from, to, func(from, to *scpb.PartitionZoneConfig) bool {
					return from.SeqNum < to.SeqNum
				}),
			}
		})
}
