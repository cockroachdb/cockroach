// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcevent

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// rowFetcherCache maintains a cache of single table row.Fetchers. Given a key
// with an mvcc timestamp, it retrieves the correct TableDescriptor for that key
// and returns a row.Fetcher initialized with that table. This Fetcher's
// ConsumeKVProvider() can be used to turn that key (or all the keys making up
// the column families of one row) into a row.
type rowFetcherCache struct {
	codec           keys.SQLCodec
	leaseMgr        *lease.Manager
	fetchers        *cache.UnorderedCache
	watchedFamilies map[watchedFamily]struct{}

	collection *descs.Collection
	db         *kv.DB

	a tree.DatumAlloc
}

type cachedFetcher struct {
	tableDesc  catalog.TableDescriptor
	fetcher    row.Fetcher
	familyDesc descpb.ColumnFamilyDescriptor
	skip       bool
}

type watchedFamily struct {
	tableID    descpb.ID
	familyName string
}

// newRowFetcherCache constructs row fetcher cache.
func newRowFetcherCache(
	ctx context.Context,
	codec keys.SQLCodec,
	leaseMgr *lease.Manager,
	cf *descs.CollectionFactory,
	db *kv.DB,
	targets changefeedbase.Targets,
) (*rowFetcherCache, error) {
	if targets.Size == 0 {
		return nil, errors.AssertionFailedf("Expected at least one target, found 0")
	}
	watchedFamilies := make(map[watchedFamily]struct{}, targets.Size)
	err := targets.EachTarget(func(t changefeedbase.Target) error {
		watchedFamilies[watchedFamily{tableID: t.TableID, familyName: t.FamilyName}] = struct{}{}
		return nil
	})
	if len(watchedFamilies) == 0 {
		return nil, errors.AssertionFailedf("No watched families resulted from %+v", targets)
	}
	return &rowFetcherCache{
		codec:           codec,
		leaseMgr:        leaseMgr,
		collection:      cf.NewCollection(ctx),
		db:              db,
		fetchers:        cache.NewUnorderedCache(DefaultCacheConfig),
		watchedFamilies: watchedFamilies,
	}, err
}

func refreshUDT(
	ctx context.Context, tableID descpb.ID, db *kv.DB, collection *descs.Collection, ts hlc.Timestamp,
) (tableDesc catalog.TableDescriptor, err error) {
	// If the table contains user defined types, then use the
	// descs.Collection to retrieve a TableDescriptor with type metadata
	// hydrated. We open a transaction here only because the
	// descs.Collection needs one to get a read timestamp. We do this lookup
	// again behind a conditional to avoid allocating any transaction
	// metadata if the table has user defined types. This can be bypassed
	// once (#53751) is fixed. Once the descs.Collection can take in a read
	// timestamp rather than a whole transaction, we can use the
	// descs.Collection directly here.
	// TODO (SQL Schema): #53751.
	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := txn.SetFixedTimestamp(ctx, ts)
		if err != nil {
			return err
		}
		tableDesc, err = collection.ByIDWithLeased(txn).WithoutNonPublic().Get().Table(ctx, tableID)
		return err
	}); err != nil {
		if errors.Is(err, catalog.ErrDescriptorDropped) {
			// Dropped descriptors are a bad news.
			return nil, changefeedbase.WithTerminalError(err)
		}

		// Manager can return all kinds of errors during chaos, but based on
		// its usage, none of them should ever be terminal.
		return nil, changefeedbase.MarkRetryableError(err)
	}
	// Immediately release the lease, since we only need it for the exact
	// timestamp requested.
	collection.ReleaseAll(ctx)
	return tableDesc, nil
}

func (c *rowFetcherCache) tableDescForKey(
	ctx context.Context, key roachpb.Key, ts hlc.Timestamp,
) (catalog.TableDescriptor, descpb.FamilyID, error) {
	var tableDesc catalog.TableDescriptor
	key, err := c.codec.StripTenantPrefix(key)
	if err != nil {
		return nil, descpb.FamilyID(0), err
	}
	remaining, tableID, _, err := rowenc.DecodePartialTableIDIndexID(key)
	if err != nil {
		return nil, descpb.FamilyID(0), err
	}

	familyID, err := keys.DecodeFamilyKey(key)
	if err != nil {
		return nil, descpb.FamilyID(0), err
	}

	family := descpb.FamilyID(familyID)

	// Retrieve the target TableDescriptor from the lease manager. No caching
	// is attempted because the lease manager does its own caching.
	desc, err := c.leaseMgr.Acquire(ctx, ts, tableID)
	if err != nil {
		// Manager can return all kinds of errors during chaos, but based on
		// its usage, none of them should ever be terminal.
		return nil, family, changefeedbase.MarkRetryableError(err)
	}
	tableDesc = desc.Underlying().(catalog.TableDescriptor)
	// Immediately release the lease, since we only need it for the exact
	// timestamp requested.
	desc.Release(ctx)
	if catalog.MaybeRequiresHydration(tableDesc) {
		tableDesc, err = refreshUDT(ctx, tableID, c.db, c.collection, ts)
		if err != nil {
			return nil, family, err
		}
	}

	// Skip over the column data.
	for skippedCols := 0; skippedCols < tableDesc.GetPrimaryIndex().NumKeyColumns(); skippedCols++ {
		l, err := encoding.PeekLength(remaining)
		if err != nil {
			return nil, family, err
		}
		remaining = remaining[l:]
	}

	return tableDesc, family, nil
}

// ErrUnwatchedFamily is a sentinel error that indicates this part of the row
// is not being watched and does not need to be decoded.
var ErrUnwatchedFamily = errors.New("watched table but unwatched family")

// RowFetcherForColumnFamily returns row.Fetcher for the specified column family.
// Returns ErrUnwatchedFamily error if family is not watched.
func (c *rowFetcherCache) RowFetcherForColumnFamily(
	tableDesc catalog.TableDescriptor,
	family descpb.FamilyID,
	sysCols []descpb.ColumnDescriptor,
	keyOnly bool,
) (*row.Fetcher, *descpb.ColumnFamilyDescriptor, error) {
	idVer := CacheKey{ID: tableDesc.GetID(), Version: tableDesc.GetVersion(), FamilyID: family}
	if v, ok := c.fetchers.Get(idVer); ok {
		f := v.(*cachedFetcher)
		if f.skip {
			return nil, nil, ErrUnwatchedFamily
		}
		// Ensure that all user defined types are up to date with the cached
		// version and the desired version to use the cache. It is safe to use
		// UserDefinedTypeColsHaveSameVersion if we have a hit because we are
		// guaranteed that the tables have the same version. Additionally, these
		// fetchers are always initialized with a single tabledesc.Get.
		if safe, err := catalog.UserDefinedTypeColsInFamilyHaveSameVersion(tableDesc, f.tableDesc, family); err != nil {
			return nil, nil, err
		} else if safe {
			return &f.fetcher, &f.familyDesc, nil
		}
	}

	familyDesc, err := catalog.MustFindFamilyByID(tableDesc, family)
	if err != nil {
		return nil, nil, err
	}

	f := &cachedFetcher{
		tableDesc:  tableDesc,
		familyDesc: *familyDesc,
	}
	rf := &f.fetcher

	_, wholeTableWatched := c.watchedFamilies[watchedFamily{tableID: tableDesc.GetID()}]
	if !wholeTableWatched {
		_, familyWatched := c.watchedFamilies[watchedFamily{tableID: tableDesc.GetID(), familyName: familyDesc.Name}]
		if !familyWatched {
			f.skip = true
			return nil, nil, ErrUnwatchedFamily
		}
	}

	var spec fetchpb.IndexFetchSpec

	var relevantColumns descpb.ColumnIDs
	if keyOnly {
		relevantColumns = tableDesc.GetPrimaryIndex().CollectKeyColumnIDs().Ordered()
	} else {
		relevantColumns, err = getRelevantColumnsForFamily(tableDesc, familyDesc)
	}
	if err != nil {
		return nil, nil, err
	}

	if err := rowenc.InitIndexFetchSpec(
		&spec, c.codec, tableDesc, tableDesc.GetPrimaryIndex(), relevantColumns,
	); err != nil {
		return nil, nil, err
	}

	// Add system columns.
	for _, sc := range sysCols {
		spec.FetchedColumns = append(spec.FetchedColumns, fetchpb.IndexFetchSpec_Column{
			ColumnID:      sc.ID,
			Name:          sc.Name,
			Type:          sc.Type,
			IsNonNullable: !sc.Nullable,
		})
	}

	if err := rf.Init(
		context.TODO(),
		row.FetcherInitArgs{
			WillUseKVProvider: true,
			Alloc:             &c.a,
			Spec:              &spec,
		},
	); err != nil {
		return nil, nil, err
	}

	c.fetchers.Add(idVer, f)
	return rf, familyDesc, nil
}
