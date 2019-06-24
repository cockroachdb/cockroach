// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// InsertNewStat inserts a new statistic in the system table.
// The caller is responsible for calling GossipTableStatAdded to notify the stat
// caches.
func InsertNewStat(
	ctx context.Context,
	executor sqlutil.InternalExecutor,
	txn *client.Txn,
	tableID sqlbase.ID,
	name string,
	columnIDs []sqlbase.ColumnID,
	rowCount, distinctCount, nullCount int64,
	h *HistogramData,
) error {
	// We must pass a nil interface{} if we want to insert a NULL.
	var nameVal, histogramVal interface{}
	if name != "" {
		nameVal = name
	}
	if h != nil {
		var err error
		histogramVal, err = protoutil.Marshal(h)
		if err != nil {
			return err
		}
	}

	columnIDsVal := tree.NewDArray(types.Int)
	for _, c := range columnIDs {
		if err := columnIDsVal.Append(tree.NewDInt(tree.DInt(int(c)))); err != nil {
			return err
		}
	}

	_, err := executor.Exec(
		ctx, "insert-statistic", txn,
		`INSERT INTO system.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		tableID,
		nameVal,
		columnIDsVal,
		rowCount,
		distinctCount,
		nullCount,
		histogramVal,
	)
	return err
}

// GossipTableStatAdded causes the statistic caches for this table to be
// invalidated.
func GossipTableStatAdded(g *gossip.Gossip, tableID sqlbase.ID) error {
	// TODO(radu): perhaps use a TTL here to avoid having a key per table floating
	// around forever (we would need the stat cache to evict old entries
	// automatically though).
	return g.AddInfo(
		gossip.MakeTableStatAddedKey(uint32(tableID)),
		nil, /* value */
		0,   /* ttl */
	)
}
