// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// tombstoneUpdater is a helper for updating the mvcc origin timestamp assigned
// to a tombstone. It internally manages leasing a descriptor for a single
// table. The lease should be released by calling ReleaseLeases.
type tombstoneUpdater struct {
	codec    keys.SQLCodec
	db       *kv.DB
	leaseMgr *lease.Manager
	sd       *sessiondata.SessionData
	settings *cluster.Settings
	descID   descpb.ID

	// leased holds fields whose lifetimes are tied to a leased descriptor.
	leased struct {
		// descriptor is a leased descriptor. Callers should use getDeleter to
		// ensure the lease is valid for the current transaction.
		descriptor lease.LeasedDescriptor
		// deleter is a row.Deleter that uses the leased descriptor. Callers should
		// use getDeleter to ensure the lease is valid for the current transaction.
		deleter row.Deleter
		// columns are the name of the columns that are expected in the cdc row.
		columns []string
	}

	// scratch is a scratch buffer for the tombstone updater. This is reused
	// across calls to addToBatch to avoid allocations.
	scratch []tree.Datum
}

func (c *tombstoneUpdater) ReleaseLeases(ctx context.Context) {
	// NOTE: ReleaseLeases may be called multiple times since its called if the lease
	// expires and a new lease is acquired.
	if c.leased.descriptor != nil {
		c.leased.descriptor.Release(ctx)
		c.leased.descriptor = nil
		c.leased.deleter = row.Deleter{}
		c.leased.columns = c.leased.columns[:0]
	}
}

func newTombstoneUpdater(
	codec keys.SQLCodec,
	db *kv.DB,
	leaseMgr *lease.Manager,
	descID descpb.ID,
	sd *sessiondata.SessionData,
	settings *cluster.Settings,
) *tombstoneUpdater {
	return &tombstoneUpdater{
		codec:    codec,
		db:       db,
		leaseMgr: leaseMgr,
		descID:   descID,
		sd:       sd,
		settings: settings,
	}
}

// updateTombstone attempts to update the tombstone for the given row. This is
// expected to always succeed. The delete will only return zero rows if the
// operation loses LWW or the row does not exist. So if the cput fails on a
// condition, it should also fail on LWW, which is treated as a success.
func (tu *tombstoneUpdater) updateTombstone(
	ctx context.Context, txn isql.Txn, afterRow cdcevent.Row,
) (batchStats, error) {
	err := func() error {
		if txn != nil {
			// If updateTombstone is called in a transaction, create and run a batch
			// in the transaction.
			batch := txn.KV().NewBatch()
			if err := tu.addToBatch(ctx, txn.KV(), batch, afterRow); err != nil {
				return err
			}
			return txn.KV().Run(ctx, batch)
		}
		// If updateTombstone is called outside of a transaction, create and run a
		// 1pc transaction.
		return tu.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			batch := txn.NewBatch()
			if err := tu.addToBatch(ctx, txn, batch, afterRow); err != nil {
				return err
			}
			return txn.CommitInBatch(ctx, batch)
		})
	}()
	if err != nil && isLwwLoser(err) {
		if isLwwLoser(err) {
			return batchStats{kvWriteTooOld: 1}, nil
		}
		return batchStats{}, err
	}
	return batchStats{}, nil
}

func (tu *tombstoneUpdater) addToBatch(
	ctx context.Context, txn *kv.Txn, batch *kv.Batch, afterRow cdcevent.Row,
) error {
	deleter, err := tu.getDeleter(ctx, txn)
	if err != nil {
		return err
	}

	tu.scratch = tu.scratch[:0]

	// Note that the columns in the cdcevent row are decoded using a descriptor
	// for the source column, whereas the row.Deleter column list is initialized
	// using columns from the destination table. This is funky, but it works
	// because we validate LDR schemas are compatible.

	datums, err := afterRow.DatumsNamed(tu.leased.columns)
	if err != nil {
		return err
	}
	if err := datums.Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		tu.scratch = append(tu.scratch, d)
		return nil
	}); err != nil {
		return err
	}

	// the index helpers are never really used since we are always updating a
	// tombstone.
	var ph row.PartialIndexUpdateHelper
	var vh row.VectorIndexUpdateHelper

	return deleter.DeleteRow(
		ctx,
		batch,
		tu.scratch,
		ph,
		vh,
		&row.OriginTimestampCPutHelper{
			OriginTimestamp:    afterRow.MvccTimestamp,
			PreviousWasDeleted: true,
		},
		false, /* mustValidateOldPKValues */
		false, /* traceKV */
	)
}

func (tu *tombstoneUpdater) getDeleter(ctx context.Context, txn *kv.Txn) (row.Deleter, error) {
	timestamp := txn.ProvisionalCommitTimestamp()
	if tu.leased.descriptor == nil || !timestamp.After(tu.leased.descriptor.Expiration(ctx)) {
		tu.ReleaseLeases(ctx)

		var err error
		tu.leased.descriptor, err = tu.leaseMgr.Acquire(ctx, timestamp, tu.descID)
		if err != nil {
			return row.Deleter{}, err
		}

		cols, err := writeableColunms(ctx, tu.leased.descriptor.Underlying().(catalog.TableDescriptor))
		if err != nil {
			return row.Deleter{}, err
		}

		for _, col := range cols {
			tu.leased.columns = append(tu.leased.columns, col.GetName())
		}

		tu.leased.deleter = row.MakeDeleter(tu.codec, tu.leased.descriptor.Underlying().(catalog.TableDescriptor), nil /* lockedIndexes */, cols, tu.sd, &tu.settings.SV, nil /* metrics */)
	}
	if err := txn.UpdateDeadline(ctx, tu.leased.descriptor.Expiration(ctx)); err != nil {
		return row.Deleter{}, err
	}
	return tu.leased.deleter, nil
}
