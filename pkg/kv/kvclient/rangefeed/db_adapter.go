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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	withDiff bool,
	eventC chan<- *roachpb.RangeFeedEvent,
) error {
	return dbc.distSender.RangeFeed(ctx, spans, startFrom, withDiff, eventC)
}

// Scan is part of the kvDB interface.
func (dbc *dbAdapter) Scan(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	parallelismFn func() int,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}

	if len(spans) == 1 {
		return dbc.scanSpan(ctx, spans[0], asOf, rowFn)
	}

	if parallelismFn == nil {
		parallelismFn = func() int { return 1 }
	} else {
		highParallelism := log.Every(30 * time.Second)
		userSuppliedFn := parallelismFn
		parallelismFn = func() int {
			p := userSuppliedFn()
			if p < 1 {
				p = 1
			}
			if p > 100 && highParallelism.ShouldLog() {
				log.Warningf(ctx, "high scan parallelism %d", p)
				// Should this be capped?
			}
			return p
		}
	}

	spansToProcess, err := allRangeSpans(ctx, dbc.distSender, spans)
	if err != nil {
		return err
	}

	currentScanLimit := parallelismFn()
	exportLim := limit.MakeConcurrentRequestLimiter("rangefeedScanLimiter", parallelismFn())
	g := ctxgroup.WithContext(ctx)

	for _, span := range spansToProcess {
		if newLimit := parallelismFn(); newLimit != currentScanLimit {
			currentScanLimit = newLimit
			exportLim.SetLimit(newLimit)
		}

		limAlloc, err := exportLim.Begin(ctx)
		if err != nil {
			return errors.CombineErrors(err, g.Wait())
		}

		sp := span
		g.GoCtx(func(ctx context.Context) error {
			defer limAlloc.Release()
			return dbc.scanSpan(ctx, sp, asOf, rowFn)
		})
	}

	return g.Wait()
}

func (dbc *dbAdapter) scanSpan(
	ctx context.Context, span roachpb.Span, asOf hlc.Timestamp, rowFn func(value roachpb.KeyValue),
) error {
	return dbc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetFixedTimestamp(ctx, asOf); err != nil {
			return err
		}
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

func allRangeSpans(
	ctx context.Context, ds *kvcoord.DistSender, spans []roachpb.Span,
) ([]roachpb.Span, error) {

	var sg roachpb.SpanGroup

	it := kvcoord.NewRangeIterator(ds)
	for i := range spans {
		rSpan, err := keys.SpanAddr(spans[i])
		if err != nil {
			return nil, err
		}
		for it.Seek(ctx, rSpan.Key, kvcoord.Ascending); ; it.Next(ctx) {
			if !it.Valid() {
				return nil, it.Error()
			}
			sg.Add(roachpb.Span{
				Key: it.Desc().StartKey.AsRawKey(), EndKey: it.Desc().EndKey.AsRawKey(),
			})
			if !it.NeedAnother(rSpan) {
				break
			}
		}
	}
	return sg.Slice(), nil
}
