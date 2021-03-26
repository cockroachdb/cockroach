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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// InsertNewStats inserts a slice of statistics at the current time into the
// system table.
func InsertNewStats(
	ctx context.Context,
	executor sqlutil.InternalExecutor,
	txn *kv.Txn,
	tableStats []*TableStatisticProto,
) error {
	var err error
	for _, statistic := range tableStats {
		err = InsertNewStat(
			ctx,
			executor,
			txn,
			statistic.TableID,
			statistic.Name,
			statistic.ColumnIDs,
			int64(statistic.RowCount),
			int64(statistic.DistinctCount),
			int64(statistic.NullCount),
			statistic.HistogramData,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertNewStat inserts a new statistic in the system table.
// The caller is responsible for calling GossipTableStatAdded to notify the stat
// caches.
func InsertNewStat(
	ctx context.Context,
	executor sqlutil.InternalExecutor,
	txn *kv.Txn,
	tableID descpb.ID,
	name string,
	columnIDs []descpb.ColumnID,
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
//
// Note that we no longer use gossip to keep the cache up-to-date, but we still
// send the updates for mixed-version clusters during upgrade.
//
// TODO(radu): remove this in 22.1.
func GossipTableStatAdded(g *gossip.Gossip, tableID descpb.ID) error {
	// TODO(radu): perhaps use a TTL here to avoid having a key per table floating
	// around forever (we would need the stat cache to evict old entries
	// automatically though).
	return g.AddInfo(
		gossip.MakeTableStatAddedKey(uint32(tableID)),
		nil, /* value */
		0,   /* ttl */
	)
}
