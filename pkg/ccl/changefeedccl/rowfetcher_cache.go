// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// rowFetcherCache maintains a cache of single table RowFetchers. Given a key
// with an mvcc timestamp, it retrieves the correct TableDescriptor for that key
// and returns a RowFetcher initialized with that table. This RowFetcher's
// StartScanFrom can be used to turn that key (or all the keys making up the
// column families of one row) into a row.
type rowFetcherCache struct {
	leaseMgr *sql.LeaseManager
	fetchers map[*sqlbase.TableDescriptor]*sqlbase.RowFetcher

	a sqlbase.DatumAlloc
}

func newRowFetcherCache(leaseMgr *sql.LeaseManager) *rowFetcherCache {
	return &rowFetcherCache{
		leaseMgr: leaseMgr,
		fetchers: make(map[*sqlbase.TableDescriptor]*sqlbase.RowFetcher),
	}
}

func (c *rowFetcherCache) RowFetcherForKey(
	ctx context.Context, key roachpb.Key, ts hlc.Timestamp,
) (*sqlbase.RowFetcher, error) {
	// TODO(dan): Handle interleaved tables.
	_, tableID, _, err := sqlbase.DecodeTableIDIndexID(key)
	if err != nil {
		return nil, err
	}

	// TODO(dan): We don't really need a lease, this is just a convenient way to
	// get the right descriptor for a timestamp, so release it immediately after
	// we acquire it. Avoid the lease entirely.
	tableDesc, _, err := c.leaseMgr.Acquire(ctx, ts, tableID)
	if err != nil {
		return nil, err
	}
	if err := c.leaseMgr.Release(tableDesc); err != nil {
		return nil, err
	}
	if rf, ok := c.fetchers[tableDesc]; ok {
		return rf, nil
	}

	// TODO(dan): Allow for decoding a subset of the columns.
	colIdxMap := make(map[sqlbase.ColumnID]int)
	var valNeededForCol util.FastIntSet
	for colIdx, col := range tableDesc.Columns {
		colIdxMap[col.ID] = colIdx
		valNeededForCol.Add(colIdx)
	}

	var rf sqlbase.RowFetcher
	if err := rf.Init(
		false /* reverse */, false /* returnRangeInfo */, false /* isCheck */, &c.a,
		sqlbase.RowFetcherTableArgs{
			Spans:            tableDesc.AllIndexSpans(),
			Desc:             tableDesc,
			Index:            &tableDesc.PrimaryIndex,
			ColIdxMap:        colIdxMap,
			IsSecondaryIndex: false,
			Cols:             tableDesc.Columns,
			ValNeededForCol:  valNeededForCol,
		},
	); err != nil {
		return nil, err
	}
	// TODO(dan): Bound the size of the cache. Resolved notifications will let
	// us evict anything for timestamps entirely before the notification. Then
	// probably an LRU just in case?
	c.fetchers[tableDesc] = &rf
	return &rf, nil
}
