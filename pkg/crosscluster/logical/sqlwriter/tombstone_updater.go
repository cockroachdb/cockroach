// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// OriginID1Options sets the origin ID to 1 for all replication writes. This
// prevents logical replication from picking up the tombstone update as a
// deletion event.
var OriginID1Options = &kvpb.WriteOptions{OriginID: 1}

// TombstoneUpdater is a helper for updating the mvcc origin timestamp assigned
// to a tombstone. It internally manages leasing a descriptor for a single
// table. The lease should be released by calling ReleaseLeases.
type TombstoneUpdater struct {
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
	}

	scratch []tree.Datum
}

func (c *TombstoneUpdater) ReleaseLeases(ctx context.Context) {
	// NOTE: ReleaseLeases may be called multiple times since its called if the
	// lease expires and a new lease is acquired.
	if c.leased.descriptor != nil {
		c.leased.descriptor.Release(ctx)
		c.leased.descriptor = nil
		c.leased.deleter = row.Deleter{}
	}
}

func NewTombstoneUpdater(
	codec keys.SQLCodec,
	db *kv.DB,
	leaseMgr *lease.Manager,
	descID descpb.ID,
	sd *sessiondata.SessionData,
	settings *cluster.Settings,
) *TombstoneUpdater {
	return &TombstoneUpdater{
		codec:    codec,
		db:       db,
		leaseMgr: leaseMgr,
		descID:   descID,
		sd:       sd,
		settings: settings,
	}
}

// UpdateTombstoneAny is an UpdateTombstone wrapper that accepts the []any
// datum slice from the original sql writer's datum builder.
func (tu *TombstoneUpdater) UpdateTombstoneAny(
	ctx context.Context, txn isql.Txn, mvccTimestamp hlc.Timestamp, datums []any,
) (bool, error) {
	tu.scratch = tu.scratch[:0]
	for _, datum := range datums {
		tu.scratch = append(tu.scratch, datum.(tree.Datum))
	}
	return tu.UpdateTombstone(ctx, txn, mvccTimestamp, tu.scratch)
}

// UpdateTombstone attempts to update the tombstone for the given row. This is
// expected to always succeed. The delete will only return zero rows if the
// operation loses LWW or the row does not exist. So if the cput fails on a
// condition, it should also fail on LWW, which is treated as a success.
//
// It returns true if the update lost LWW (i.e. the local tombstone was already
// newer).
func (tu *TombstoneUpdater) UpdateTombstone(
	ctx context.Context, txn isql.Txn, mvccTimestamp hlc.Timestamp, afterRow []tree.Datum,
) (bool, error) {
	err := func() error {
		if txn != nil {
			// If UpdateTombstone is called in a transaction, create and run a
			// batch in the transaction.
			batch := txn.KV().NewBatch()
			batch.Header.WriteOptions = OriginID1Options
			if err := tu.AddToBatch(ctx, txn.KV(), batch, mvccTimestamp, afterRow); err != nil {
				return err
			}
			return txn.KV().Run(ctx, batch)
		}
		// If UpdateTombstone is called outside of a transaction, create and
		// run a 1pc transaction.
		return tu.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			batch := txn.NewBatch()
			batch.Header.WriteOptions = OriginID1Options
			if err := tu.AddToBatch(ctx, txn, batch, mvccTimestamp, afterRow); err != nil {
				return err
			}
			return txn.CommitInBatch(ctx, batch)
		})
	}()
	if err != nil {
		if IsLwwLoser(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// AddToBatch adds a tombstone delete operation to the given KV batch. The
// caller is responsible for setting the batch's WriteOptions and running the
// batch.
func (tu *TombstoneUpdater) AddToBatch(
	ctx context.Context,
	txn *kv.Txn,
	batch *kv.Batch,
	mvccTimestamp hlc.Timestamp,
	afterRow []tree.Datum,
) error {
	deleter, err := tu.getDeleter(ctx, txn)
	if err != nil {
		return err
	}

	var ph row.PartialIndexUpdateHelper
	var vh row.VectorIndexUpdateHelper

	return deleter.DeleteRow(
		ctx,
		batch,
		afterRow,
		ph,
		vh,
		row.OriginTimestampCPutHelper{
			OriginTimestamp:    mvccTimestamp,
			PreviousWasDeleted: true,
		},
		false, /* mustValidateOldPKValues */
		false, /* traceKV */
	)
}

func (tu *TombstoneUpdater) hasValidLease(ctx context.Context, now hlc.Timestamp) bool {
	return tu.leased.descriptor != nil && tu.leased.descriptor.Expiration(ctx).After(now)
}

func (tu *TombstoneUpdater) getDeleter(ctx context.Context, txn *kv.Txn) (row.Deleter, error) {
	timestamp := txn.ProvisionalCommitTimestamp()
	if !tu.hasValidLease(ctx, timestamp) {
		tu.ReleaseLeases(ctx)

		var err error
		tu.leased.descriptor, err = tu.leaseMgr.Acquire(ctx, lease.TimestampToReadTimestamp(timestamp), tu.descID)
		if err != nil {
			return row.Deleter{}, err
		}

		table := tu.leased.descriptor.Underlying().(catalog.TableDescriptor)
		schema := GetColumnSchema(table)
		cols := make([]catalog.Column, len(schema))
		for i, cs := range schema {
			cols[i] = cs.Column
		}

		tu.leased.deleter = row.MakeDeleter(
			tu.codec,
			tu.leased.descriptor.Underlying().(catalog.TableDescriptor),
			nil, /* lockedIndexes */
			cols,
			tu.sd,
			&tu.settings.SV,
			nil, /* metrics */
		)
	}
	if err := txn.UpdateDeadline(ctx, tu.leased.descriptor.Expiration(ctx)); err != nil {
		return row.Deleter{}, err
	}
	return tu.leased.deleter, nil
}
