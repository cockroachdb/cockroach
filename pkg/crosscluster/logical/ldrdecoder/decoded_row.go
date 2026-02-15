// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldrdecoder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Decodedrow represents a (prev, next) row value that is constructed from a
// replication stream event. The replication stream event is encoded using the
// source table descriptor. The decoded must contain datums that are compatible
// with the destination table descriptor.
type DecodedRow struct {
	// TableID is the descriptor ID for the table in the destination cluster.
	TableID descpb.ID

	// IsDelete is true if the event is a delete. This implies values in the row
	// may be NULL that are not allowed to be NULL in the source table's schema.
	// Only the primary key columns are expected to have values.
	IsDelete bool

	// RowTimestamp is the mvcc timestamp of the row in the source cluster.
	RowTimestamp hlc.Timestamp

	// Row is the decoded Row from the replication stream. The datums in Row are
	// in col id order for the destination table.
	Row tree.Datums

	// PrevRow is either the previous row from the replication stream or it is
	// the local version of the row if there was a read refresh.
	//
	// The datums in PrevRow are in col id order for the destination table. nil
	// PrevRow may still lose LWW if there is a recent tombstone.
	PrevRow tree.Datums
}

func (d *DecodedRow) IsDeleteRow() bool {
	return d.IsDelete && len(d.PrevRow) != 0
}

func (d *DecodedRow) IsTombstoneUpdate() bool {
	return d.IsDelete && len(d.PrevRow) == 0
}

func (d *DecodedRow) IsInsertRow() bool {
	return !d.IsDelete && len(d.PrevRow) == 0
}

func (d *DecodedRow) IsUpdateRow() bool {
	return !d.IsDelete && len(d.PrevRow) > 0
}
