// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// dbAdapter is an implementation of the kvDB interface using a real *kv.DB.
type dbAdapter struct {
	db              *kv.DB
	distSender      *kvcoord.DistSender
	targetScanBytes int64
}

var _ kvDB = (*dbAdapter)(nil)

// TODO(ajwerner): Hook up a memory monitor. Fortunately most users of the
// initial scan are reading scant amounts of data.

// defaultTargetScanBytes was pulled out of thin air. The main reason is that
// this thing is not hooked up to a memory monitor.
const defaultTargetScanBytes = 1 << 19 // 512 KiB

// newDBAdapter construct a kvDB using a *kv.DB.
func newDBAdapter(db *kv.DB) (*dbAdapter, error) {
	var distSender *kvcoord.DistSender
	{
		txnWrapperSender, ok := db.NonTransactionalSender().(*kv.CrossRangeTxnWrapperSender)
		if !ok {
			return nil, errors.Errorf("failed to extract a %T from %T",
				(*kv.CrossRangeTxnWrapperSender)(nil), db.NonTransactionalSender())
		}
		distSender, ok = txnWrapperSender.Wrapped().(*kvcoord.DistSender)
		if !ok {
			return nil, errors.Errorf("failed to extract a %T from %T",
				(*kvcoord.DistSender)(nil), txnWrapperSender.Wrapped())
		}
	}
	return &dbAdapter{
		db:              db,
		distSender:      distSender,
		targetScanBytes: defaultTargetScanBytes,
	}, nil
}

// RangeFeed is part of the kvDB interface.
func (dbc *dbAdapter) RangeFeed(
	ctx context.Context,
	span roachpb.Span,
	startFrom hlc.Timestamp,
	withDiff bool,
	eventC chan<- *roachpb.RangeFeedEvent,
) error {
	return dbc.distSender.RangeFeed(ctx, span, startFrom, withDiff, eventC)
}

// Scan is part of the kvDB interface.
func (dbc *dbAdapter) Scan(
	ctx context.Context, span roachpb.Span, asOf hlc.Timestamp, rowFn func(value roachpb.KeyValue),
) error {
	return dbc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, asOf)
		sp := span
		var b kv.Batch
		for {
			b.Header.TargetBytes = dbc.targetScanBytes
			b.Scan(sp.Key, sp.EndKey)
			if err := txn.Run(ctx, &b); err != nil {
				return err
			}
			res := b.Results[0]
			for _, row := range res.Rows {
				rowFn(roachpb.KeyValue{Key: row.Key, Value: *row.Value})
			}
			if res.ResumeSpan == nil {
				return nil
			}
			sp = res.ResumeSpanAsValue()
			b = kv.Batch{}
		}
	})
}
