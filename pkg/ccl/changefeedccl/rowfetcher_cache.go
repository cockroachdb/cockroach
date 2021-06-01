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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/hydratedtables"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	codec    keys.SQLCodec
	leaseMgr *lease.Manager
	fetchers map[idVersion]*row.Fetcher

	collection *descs.Collection
	db         *kv.DB

	a rowenc.DatumAlloc
}

type idVersion struct {
	id      descpb.ID
	version descpb.DescriptorVersion
}

func newRowFetcherCache(
	ctx context.Context,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	hydratedTables *hydratedtables.Cache,
	db *kv.DB,
) *rowFetcherCache {
	return &rowFetcherCache{
		codec:      codec,
		leaseMgr:   leaseMgr,
		collection: descs.NewCollection(settings, leaseMgr, hydratedTables, nil /* virtualSchemas */),
		db:         db,
		fetchers:   make(map[idVersion]*row.Fetcher),
	}
}

func (c *rowFetcherCache) TableDescForKey(
	ctx context.Context, key roachpb.Key, ts hlc.Timestamp,
) (catalog.TableDescriptor, error) {
	var tableDesc catalog.TableDescriptor
	key, err := c.codec.StripTenantPrefix(key)
	if err != nil {
		return nil, err
	}
	for skippedCols := 0; ; {
		remaining, tableID, _, err := rowenc.DecodePartialTableIDIndexID(key)
		if err != nil {
			return nil, err
		}

		// Retrieve the target TableDescriptor from the lease manager. No caching
		// is attempted because the lease manager does its own caching.
		desc, err := c.leaseMgr.Acquire(ctx, ts, tableID)
		if err != nil {
			// Manager can return all kinds of errors during chaos, but based on
			// its usage, none of them should ever be terminal.
			return nil, changefeedbase.MarkRetryableError(err)
		}
		tableDesc = desc.Underlying().(catalog.TableDescriptor)
		// Immediately release the lease, since we only need it for the exact
		// timestamp requested.
		desc.Release(ctx)
		if tableDesc.ContainsUserDefinedTypes() {
			// If the table contains user defined types, then use the descs.Collection
			// to retrieve a TableDescriptor with type metadata hydrated. We open a
			// transaction here only because the descs.Collection needs one to get
			// a read timestamp. We do this lookup again behind a conditional to avoid
			// allocating any transaction metadata if the table has user defined types.
			// This can be bypassed once (#53751) is fixed. Once the descs.Collection can
			// take in a read timestamp rather than a whole transaction, we can use the
			// descs.Collection directly here.
			// TODO (SQL Schema): #53751.
			if err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				txn.SetFixedTimestamp(ctx, ts)
				var err error
				tableDesc, err = c.collection.GetImmutableTableByID(ctx, txn, tableID, tree.ObjectLookupFlags{})
				return err
			}); err != nil {
				// Manager can return all kinds of errors during chaos, but based on
				// its usage, none of them should ever be terminal.
				return nil, changefeedbase.MarkRetryableError(err)
			}
			// Immediately release the lease, since we only need it for the exact
			// timestamp requested.
			c.collection.ReleaseAll(ctx)
		}

		// Skip over the column data.
		for ; skippedCols < tableDesc.GetPrimaryIndex().NumKeyColumns(); skippedCols++ {
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
	tableDesc catalog.TableDescriptor,
) (*row.Fetcher, error) {
	idVer := idVersion{id: tableDesc.GetID(), version: tableDesc.GetVersion()}
	// Ensure that all user defined types are up to date with the cached
	// version and the desired version to use the cache. It is safe to use
	// UserDefinedTypeColsHaveSameVersion if we have a hit because we are
	// guaranteed that the tables have the same version. Additionally, these
	// fetchers are always initialized with a single tabledesc.Immutable.
	if rf, ok := c.fetchers[idVer]; ok &&
		catalog.UserDefinedTypeColsHaveSameVersion(tableDesc, rf.GetTables()[0].(catalog.TableDescriptor)) {
		return rf, nil
	}
	// TODO(dan): Allow for decoding a subset of the columns.
	var colIdxMap catalog.TableColMap
	var valNeededForCol util.FastIntSet
	for _, col := range tableDesc.PublicColumns() {
		colIdxMap.Set(col.GetID(), col.Ordinal())
		valNeededForCol.Add(col.Ordinal())
	}

	var rf row.Fetcher
	rfArgs := row.FetcherTableArgs{
		Spans:            tableDesc.AllIndexSpans(c.codec),
		Desc:             tableDesc,
		Index:            tableDesc.GetPrimaryIndex(),
		ColIdxMap:        colIdxMap,
		IsSecondaryIndex: false,
		Cols:             tableDesc.PublicColumns(),
		ValNeededForCol:  valNeededForCol,
	}
	if err := rf.Init(
		context.TODO(),
		c.codec,
		false, /* reverse */
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		false, /* isCheck */
		&c.a,
		nil, /* memMonitor */
		rfArgs,
	); err != nil {
		return nil, err
	}
	// TODO(dan): Bound the size of the cache. Resolved notifications will let
	// us evict anything for timestamps entirely before the notification. Then
	// probably an LRU just in case?
	c.fetchers[idVer] = &rf
	return &rf, nil
}
