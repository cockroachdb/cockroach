// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// decodedEvent is constructed from a replication stream event. The replication
// stream event is encoded using the source table descriptor. The decoded must
// contain datums that are compatible with the destination table descriptor.
type decodedEvent struct {
	// dstDescID is the descriptor ID for the table in the destination cluster.
	dstDescID descpb.ID

	// isDelete is true if the event is a delete. This implies values in the row
	// may be NULL that are not allowed to be NULL in the source table's schema.
	// Only the primary key columns are expected to have values.
	isDelete bool

	// originTimestamp is the mvcc timestamp of the row in the source cluster.
	originTimestamp hlc.Timestamp

	// row is the decoded row from the replication stream. The datums in row are
	// in col id order for the destination table.
	row tree.Datums

	// prevRow is either the previous row from the replication stream or it is
	// the local version of the row if there was a read refresh.
	//
	// The datums in prevRow are in col id order for the destination table. nil
	// prevRow may still lose LWW if there is a recent tombstone.
	prevRow tree.Datums
}
