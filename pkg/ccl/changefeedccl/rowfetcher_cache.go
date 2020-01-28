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
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// rowFetcherCache maintains a cache of single table RowFetchers. Given a key
// with an mvcc timestamp, it retrieves the correct TableDescriptor for that key
// and returns a Fetcher initialized with that table. This Fetcher's
// StartScanFrom can be used to turn that key (or all the keys making up the
// column families of one row) into a row.
type rowFetcherCache struct {
	leaseMgr *sql.LeaseManager
	fetchers map[*sqlbase.ImmutableTableDescriptor]*row.Fetcher

	a sqlbase.DatumAlloc
}

func newRowFetcherCache(leaseMgr *sql.LeaseManager) *rowFetcherCache {
	return &rowFetcherCache{
		leaseMgr: leaseMgr,
		fetchers: make(map[*sqlbase.ImmutableTableDescriptor]*row.Fetcher),
	}
}

func (c *rowFetcherCache) TableDescForKey(
	ctx context.Context, key roachpb.Key, ts hlc.Timestamp,
) (*sqlbase.ImmutableTableDescriptor, error) {
	var tableDesc *sqlbase.ImmutableTableDescriptor
	for skippedCols := 0; ; {
		remaining, tableID, _, err := sqlbase.DecodeTableIDIndexID(key)
		if err != nil {
			return nil, err
		}
		// No caching of these are attempted, since the lease manager does its
		// own caching.
		tableDesc, _, err = c.leaseMgr.Acquire(ctx, ts, tableID)
		if err != nil {
			// LeaseManager can return all kinds of errors during chaos, but based on
			// its usage, none of them should ever be terminal.
			return nil, MarkRetryableError(err)
		}
		// Immediately release the lease, since we only need it for the exact
		// timestamp requested.
		if err := c.leaseMgr.Release(tableDesc); err != nil {
			return nil, err
		}

		// Skip over the column data.
		for ; skippedCols < len(tableDesc.PrimaryIndex.ColumnIDs); skippedCols++ {
			l, err := encoding.PeekLength(remaining)
			if err != nil {
				return nil, err
			}
			remaining = remaining[l:]
		}
		var interleaved bool
		remaining, interleaved = encoding.DecodeIfInterleavedSentinel(remaining)
		if !interleaved {
			break
		}
		key = remaining
	}

	return tableDesc, nil
}

func (c *rowFetcherCache) RowFetcherForTableDesc(
	tableDesc *sqlbase.ImmutableTableDescriptor,
) (*row.Fetcher, error) {
	if rf, ok := c.fetchers[tableDesc]; ok {
		return rf, nil
	}

	// TODO(dan): Allow for decoding a subset of the columns.
	colIdxMap := make(map[sqlbase.ColumnID]int)
	var valNeededForCol util.FastIntSet
	for colIdx := range tableDesc.Columns {
		colIdxMap[tableDesc.Columns[colIdx].ID] = colIdx
		valNeededForCol.Add(colIdx)
	}

	var rf row.Fetcher
	if err := rf.Init(
		false, /* reverse */
		sqlbase.ScanLockingStrength_FOR_NONE,
		false, /* returnRangeInfo */
		false, /* isCheck */
		&c.a,
		row.FetcherTableArgs{
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
