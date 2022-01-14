// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeedadaptor

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// DBAdapter is an implementation of the rangefeed.KVDB interface using a real
// *kv.DB.
type DBAdapter struct {
	db         *kv.DB
	st         *cluster.Settings
	distSender *kvcoord.DistSender
}

var _ rangefeed.KVDB = (*DBAdapter)(nil)

var maxScanParallelism = settings.RegisterIntSetting(
	settings.TenantWritable,
	"kv.rangefeed.max_scan_parallelism",
	"maximum number of concurrent scan requests that can be issued during initial scan",
	64,
)

// New constructs a DBAdaptor.
func New(db *kv.DB, st *cluster.Settings) (*DBAdapter, error) {
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
	return &DBAdapter{
		db:         db,
		st:         st,
		distSender: distSender,
	}, nil
}

// RangeFeed is part of the rangefeed.KVDB interface.
func (dbc *DBAdapter) RangeFeed(
	ctx context.Context,
	spans []roachpb.Span,
	startFrom hlc.Timestamp,
	withDiff bool,
	eventC chan<- *roachpb.RangeFeedEvent,
) error {
	return dbc.distSender.RangeFeed(ctx, spans, startFrom, withDiff, eventC)
}

// concurrentBoundAccount is a thread safe bound account.
type concurrentBoundAccount struct {
	syncutil.Mutex
	*mon.BoundAccount
}

func (ba *concurrentBoundAccount) Grow(ctx context.Context, x int64) error {
	ba.Lock()
	defer ba.Unlock()
	return ba.BoundAccount.Grow(ctx, x)
}

func (ba *concurrentBoundAccount) Shrink(ctx context.Context, x int64) {
	ba.Lock()
	defer ba.Unlock()
	ba.BoundAccount.Shrink(ctx, x)
}

// Scan is part of the rangefeed.KVDB interface.
func (dbc *DBAdapter) Scan(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	cfg rangefeed.ScanConfig,
) error {
	if len(spans) == 0 {
		return errors.AssertionFailedf("expected at least 1 span, got none")
	}

	var acc *concurrentBoundAccount
	if cfg.Mon != nil {
		ba := cfg.Mon.MakeBoundAccount()
		defer ba.Close(ctx)
		acc = &concurrentBoundAccount{BoundAccount: &ba}
	}

	// If we don't have parallelism configured, just scan each span in turn.
	if cfg.ScanParallelism == nil {
		for _, sp := range spans {
			if err := dbc.scanSpan(ctx, sp, asOf, rowFn, cfg.TargetScanBytes, cfg.OnSpanDone, acc); err != nil {
				return err
			}
		}
		return nil
	}

	parallelismFn := cfg.ScanParallelism
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
			maxP := int(maxScanParallelism.Get(&dbc.st.SV))
			if p > maxP {
				if highParallelism.ShouldLog() {
					log.Warningf(ctx,
						"high scan parallelism %d limited via 'kv.rangefeed.max_scan_parallelism' to %d", p, maxP)
				}
				p = maxP
			}
			return p
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g := ctxgroup.WithContext(ctx)
	err := dbc.divideAndSendScanRequests(
		ctx, &g, spans, asOf, rowFn,
		parallelismFn, cfg.TargetScanBytes, cfg.OnSpanDone, acc)
	if err != nil {
		cancel()
	}
	return errors.CombineErrors(err, g.Wait())
}

func (dbc *DBAdapter) scanSpan(
	ctx context.Context,
	span roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	targetScanBytes int64,
	onScanDone rangefeed.OnScanCompleted,
	acc *concurrentBoundAccount,
) error {
	if acc != nil {
		if err := acc.Grow(ctx, targetScanBytes); err != nil {
			return err
		}
		defer acc.Shrink(ctx, targetScanBytes)
	}

	return dbc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetFixedTimestamp(ctx, asOf); err != nil {
			return err
		}
		sp := span
		var b kv.Batch
		for {
			b.Header.TargetBytes = targetScanBytes
			b.Scan(sp.Key, sp.EndKey)
			if err := txn.Run(ctx, &b); err != nil {
				return err
			}
			res := b.Results[0]
			for _, row := range res.Rows {
				rowFn(roachpb.KeyValue{Key: row.Key, Value: *row.Value})
			}
			if res.ResumeSpan == nil {
				if onScanDone != nil {
					return onScanDone(ctx, sp)
				}
				return nil
			}

			if onScanDone != nil {
				if err := onScanDone(ctx, roachpb.Span{Key: sp.Key, EndKey: res.ResumeSpan.Key}); err != nil {
					return err
				}
			}

			sp = res.ResumeSpanAsValue()
			b = kv.Batch{}
		}
	})
}

// divideAndSendScanRequests divides spans into small ranges based on range boundaries,
// and adds those scan requests to the workGroup.  The caller is expected to wait for
// the workGroup completion, or to cancel the work group in case of an error.
func (dbc *DBAdapter) divideAndSendScanRequests(
	ctx context.Context,
	workGroup *ctxgroup.Group,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	parallelismFn func() int,
	targetScanBytes int64,
	onSpanDone rangefeed.OnScanCompleted,
	acc *concurrentBoundAccount,
) error {
	// Build a span group so that we can iterate spans in order.
	var sg roachpb.SpanGroup
	sg.Add(spans...)

	currentScanLimit := parallelismFn()
	exportLim := limit.MakeConcurrentRequestLimiter("rangefeedScanLimiter", parallelismFn())
	ri := kvcoord.MakeRangeIterator(dbc.distSender)

	for _, sp := range sg.Slice() {
		nextRS, err := keys.SpanAddr(sp)
		if err != nil {
			return err
		}

		for ri.Seek(ctx, nextRS.Key, kvcoord.Ascending); ri.Valid(); ri.Next(ctx) {
			desc := ri.Desc()
			partialRS, err := nextRS.Intersect(desc)
			if err != nil {
				return err
			}
			nextRS.Key = partialRS.EndKey

			if newLimit := parallelismFn(); newLimit != currentScanLimit {
				currentScanLimit = newLimit
				exportLim.SetLimit(newLimit)
			}

			limAlloc, err := exportLim.Begin(ctx)
			if err != nil {
				return err
			}

			sp := partialRS.AsRawSpanWithNoLocals()
			workGroup.GoCtx(func(ctx context.Context) error {
				defer limAlloc.Release()
				return dbc.scanSpan(ctx, sp, asOf, rowFn, targetScanBytes, onSpanDone, acc)
			})

			if !ri.NeedAnother(nextRS) {
				break
			}
		}
		if err := ri.Error(); err != nil {
			return ri.Error()
		}
	}

	return nil
}

// TestingScanWithOptions is exposed for testing in order to call Scan with a
// rangefeed.ScanConfig extracted from the specified list of options.
func (dbc *DBAdapter) TestingScanWithOptions(
	ctx context.Context,
	spans []roachpb.Span,
	asOf hlc.Timestamp,
	rowFn func(value roachpb.KeyValue),
	opts ...rangefeed.Option,
) error {
	sc := rangefeed.TestingScanConfigWithOptions(opts)
	return dbc.Scan(ctx, spans, asOf, rowFn, sc)
}
