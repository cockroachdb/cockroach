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
	"sort"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// poller uses ExportRequest with the `ReturnSST` to repeatedly fetch every kv
// that changed between a set of timestamps and insert them into a buffer.
//
// Each poll (ie set of ExportRequests) are rate limited to be no more often
// than the `changefeed.experimental_poll_interval` setting.
type poller struct {
	settings *cluster.Settings
	db       *client.DB
	clock    *hlc.Clock
	gossip   *gossip.Gossip
	spans    []roachpb.Span
	details  jobspb.ChangefeedDetails
	buf      *buffer

	highWater hlc.Timestamp
}

func makePoller(
	settings *cluster.Settings,
	db *client.DB,
	clock *hlc.Clock,
	gossip *gossip.Gossip,
	spans []roachpb.Span,
	details jobspb.ChangefeedDetails,
	highWater hlc.Timestamp,
	buf *buffer,
) *poller {
	return &poller{
		settings:  settings,
		db:        db,
		clock:     clock,
		gossip:    gossip,
		highWater: highWater,
		spans:     spans,
		details:   details,
		buf:       buf,
	}
}

func fetchSpansForTargets(
	ctx context.Context, db *client.DB, targets jobspb.ChangefeedTargets, ts hlc.Timestamp,
) ([]roachpb.Span, error) {
	var spans []roachpb.Span
	err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		spans = nil
		txn.SetFixedTimestamp(ctx, ts)
		// Note that all targets are currently guaranteed to be tables.
		for tableID := range targets {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
			if err != nil {
				return err
			}
			spans = append(spans, tableDesc.PrimaryIndexSpan())
		}
		return nil
	})
	return spans, err
}

// Run repeatedly polls and inserts changed kvs and resolved timestamps into a
// buffer. It blocks forever and is intended to be run in a goroutine.
//
// During each poll, a new high-water mark is chosen. The relevant spans for the
// configured tables are broken up by (possibly stale) range boundaries and
// every changed KV between the old and new high-water is fetched via
// ExportRequests. It backpressures sending the requests such that some maximum
// number are inflight or being inserted into the buffer. Finally, after each
// poll completes, a resolved timestamp notification is added to the buffer.
func (p *poller) Run(ctx context.Context) error {
	if storage.RangefeedEnabled.Get(&p.settings.SV) {
		return p.runUsingRangefeeds(ctx)
	}

	sender := p.db.NonTransactionalSender()
	for {
		pollDuration := changefeedPollInterval.Get(&p.settings.SV)
		pollDuration = pollDuration - timeutil.Since(timeutil.Unix(0, p.highWater.WallTime))
		if pollDuration > 0 {
			log.VEventf(ctx, 1, `sleeping for %s`, pollDuration)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollDuration):
			}
		}

		nextHighWater := p.clock.Now()
		log.VEventf(ctx, 1, `changefeed poll [%s,%s): %s`,
			p.highWater, nextHighWater, time.Duration(nextHighWater.WallTime-p.highWater.WallTime))

		var ranges []roachpb.RangeDescriptor
		if err := p.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			var err error
			ranges, err = allRangeDescriptors(ctx, txn)
			return err
		}); err != nil {
			return errors.Wrap(err, "fetching range descriptors")
		}

		type spanMarker struct{}
		type rangeMarker struct{}

		var spanCovering intervalccl.Covering
		for _, span := range p.spans {
			spanCovering = append(spanCovering, intervalccl.Range{
				Start:   []byte(span.Key),
				End:     []byte(span.EndKey),
				Payload: spanMarker{},
			})
		}

		var rangeCovering intervalccl.Covering
		for _, rangeDesc := range ranges {
			rangeCovering = append(rangeCovering, intervalccl.Range{
				Start:   []byte(rangeDesc.StartKey),
				End:     []byte(rangeDesc.EndKey),
				Payload: rangeMarker{},
			})
		}

		chunks := intervalccl.OverlapCoveringMerge(
			[]intervalccl.Covering{spanCovering, rangeCovering},
		)

		var requests []roachpb.Span
		for _, chunk := range chunks {
			if _, ok := chunk.Payload.([]interface{})[0].(spanMarker); !ok {
				continue
			}
			requests = append(requests, roachpb.Span{Key: chunk.Start, EndKey: chunk.End})
		}

		maxConcurrentExports := clusterNodeCount(p.gossip) *
			int(storage.ExportRequestsLimit.Get(&p.settings.SV))
		exportsSem := make(chan struct{}, maxConcurrentExports)

		var atomicFinished int64

		g := ctxgroup.WithContext(ctx)
		for _, span := range requests {
			span := span

			select {
			case <-ctx.Done():
				return ctx.Err()
			case exportsSem <- struct{}{}:
			}

			g.GoCtx(func(ctx context.Context) error {
				defer func() { <-exportsSem }()
				if log.V(2) {
					log.Infof(ctx, `sending ExportRequest [%s,%s)`, span.Key, span.EndKey)
				}
				header := roachpb.Header{Timestamp: nextHighWater}
				req := &roachpb.ExportRequest{
					RequestHeader: roachpb.RequestHeaderFromSpan(span),
					StartTime:     p.highWater,
					MVCCFilter:    roachpb.MVCCFilter_All,
					ReturnSST:     true,
					OmitChecksum:  true,
				}
				if req.StartTime == (hlc.Timestamp{}) {
					req.MVCCFilter = roachpb.MVCCFilter_Latest
				}
				startTime := timeutil.Now()
				res, pErr := client.SendWrappedWith(ctx, sender, header, req)
				finished := atomic.AddInt64(&atomicFinished, 1)
				if log.V(2) {
					log.Infof(ctx, `finished ExportRequest [%s,%s) %d of %d took %s`,
						span.Key, span.EndKey, finished, len(requests), timeutil.Since(startTime))
				}
				if pErr != nil {
					return errors.Wrapf(
						pErr.GoError(), `fetching changes for [%s,%s)`, span.Key, span.EndKey)
				}
				startTime = timeutil.Now()
				for _, file := range res.(*roachpb.ExportResponse).Files {
					if err := p.slurpSST(ctx, file.SST); err != nil {
						return err
					}
				}
				if err := p.buf.AddResolved(ctx, span, nextHighWater); err != nil {
					return err
				}
				if log.V(2) {
					log.Infof(ctx, `finished buffering [%s,%s) took %s`,
						span.Key, span.EndKey, timeutil.Since(startTime))
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}

		p.highWater = nextHighWater
	}
}

// slurpSST iterates an encoded sst and inserts the contained kvs into the
// buffer.
func (p *poller) slurpSST(ctx context.Context, sst []byte) error {
	var previousKey roachpb.Key
	var kvs []roachpb.KeyValue
	slurpKVs := func() error {
		sort.Sort(byValueTimestamp(kvs))
		for _, kv := range kvs {
			if err := p.buf.AddKV(ctx, kv); err != nil {
				return err
			}
		}
		previousKey = previousKey[:0]
		kvs = kvs[:0]
		return nil
	}

	var scratch bufalloc.ByteAllocator
	it, err := engineccl.NewMemSSTIterator(sst, false /* verify */)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Seek(engine.NilKey); ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		unsafeKey := it.UnsafeKey()
		var key roachpb.Key
		var value []byte
		scratch, key = scratch.Copy(unsafeKey.Key, 0 /* extraCap */)
		scratch, value = scratch.Copy(it.UnsafeValue(), 0 /* extraCap */)

		// The buffer currently requires that each key's mvcc revisions are
		// added in increasing timestamp order. The sst is guaranteed to be in
		// key order, but decresing timestamp order. So, buffer up kvs until the
		// key changes, then sort by increasing timestamp before handing them
		// all to AddKV.
		if !previousKey.Equal(key) {
			if err := slurpKVs(); err != nil {
				return err
			}
			previousKey = key
		}
		kvs = append(kvs, roachpb.KeyValue{
			Key:   key,
			Value: roachpb.Value{RawBytes: value, Timestamp: unsafeKey.Timestamp},
		})
	}

	return slurpKVs()
}

// TODO(nvanbenschoten): this should probably be a whole different type that
// shares a common interface with poller.
func (p *poller) runUsingRangefeeds(ctx context.Context) error {
	g := ctxgroup.WithContext(ctx)
	sender := p.db.NonTransactionalSender()
	if p.highWater == (hlc.Timestamp{}) {
		// TODO(nvanbenschoten/danhhz): This should be replaced by a series of
		// ScanRequests and this structure should be completely reworked. Right
		// now it's copied verbatim from above.
		var ranges []roachpb.RangeDescriptor
		if err := p.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			txn.SetFixedTimestamp(ctx, p.details.StatementTime)
			var err error
			ranges, err = allRangeDescriptors(ctx, txn)
			return err
		}); err != nil {
			return errors.Wrap(err, "fetching range descriptors")
		}

		type spanMarker struct{}
		type rangeMarker struct{}

		var spanCovering intervalccl.Covering
		for _, span := range p.spans {
			spanCovering = append(spanCovering, intervalccl.Range{
				Start:   []byte(span.Key),
				End:     []byte(span.EndKey),
				Payload: spanMarker{},
			})
		}

		var rangeCovering intervalccl.Covering
		for _, rangeDesc := range ranges {
			rangeCovering = append(rangeCovering, intervalccl.Range{
				Start:   []byte(rangeDesc.StartKey),
				End:     []byte(rangeDesc.EndKey),
				Payload: rangeMarker{},
			})
		}

		chunks := intervalccl.OverlapCoveringMerge(
			[]intervalccl.Covering{spanCovering, rangeCovering},
		)

		var requests []roachpb.Span
		for _, chunk := range chunks {
			if _, ok := chunk.Payload.([]interface{})[0].(spanMarker); !ok {
				continue
			}
			requests = append(requests, roachpb.Span{Key: chunk.Start, EndKey: chunk.End})
		}

		g.GoCtx(func(ctx context.Context) error {
			maxConcurrentExports := clusterNodeCount(p.gossip) *
				int(storage.ExportRequestsLimit.Get(&p.settings.SV))
			exportsSem := make(chan struct{}, maxConcurrentExports)

			var atomicFinished int64

			for _, span := range requests {
				span := span

				select {
				case <-ctx.Done():
					return ctx.Err()
				case exportsSem <- struct{}{}:
				}

				g.GoCtx(func(ctx context.Context) error {
					defer func() { <-exportsSem }()
					if log.V(2) {
						log.Infof(ctx, `sending ExportRequest [%s,%s)`, span.Key, span.EndKey)
					}
					header := roachpb.Header{Timestamp: p.details.StatementTime}
					req := &roachpb.ExportRequest{
						RequestHeader: roachpb.RequestHeaderFromSpan(span),
						StartTime:     hlc.Timestamp{},
						MVCCFilter:    roachpb.MVCCFilter_Latest,
						ReturnSST:     true,
						OmitChecksum:  true,
					}
					startTime := timeutil.Now()
					res, pErr := client.SendWrappedWith(ctx, sender, header, req)
					finished := atomic.AddInt64(&atomicFinished, 1)
					if log.V(2) {
						log.Infof(ctx, `finished ExportRequest [%s,%s) %d of %d took %s`,
							span.Key, span.EndKey, finished, len(requests), timeutil.Since(startTime))
					}
					if pErr != nil {
						return errors.Wrapf(
							pErr.GoError(), `fetching changes for [%s,%s)`, span.Key, span.EndKey)
					}
					for _, file := range res.(*roachpb.ExportResponse).Files {
						if err := p.slurpSST(ctx, file.SST); err != nil {
							return err
						}
					}
					return p.buf.AddResolved(ctx, span, p.details.StatementTime)
				})
			}
			return nil
		})
	}

	rangeFeedTS := p.details.StatementTime
	if rangeFeedTS.Less(p.highWater) {
		rangeFeedTS = p.highWater
	}

	// TODO(nvanbenschoten): This is horrible.
	ds := sender.(*client.CrossRangeTxnWrapperSender).Wrapped().(*kv.DistSender)
	eventC := make(chan *roachpb.RangeFeedEvent, 128)
	for _, span := range p.spans {
		req := &roachpb.RangeFeedRequest{
			Header: roachpb.Header{
				Timestamp: rangeFeedTS,
			},
			Span: span,
		}
		g.GoCtx(func(ctx context.Context) error {
			return ds.RangeFeed(ctx, req, eventC).GoError()
		})
	}
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case e := <-eventC:
				switch t := e.GetValue().(type) {
				case *roachpb.RangeFeedValue:
					kv := roachpb.KeyValue{Key: t.Key, Value: t.Value}
					if err := p.buf.AddKV(ctx, kv); err != nil {
						return err
					}
				case *roachpb.RangeFeedCheckpoint:
					if err := p.buf.AddResolved(ctx, t.Span, t.ResolvedTS); err != nil {
						return err
					}
				default:
					log.Fatalf(ctx, "unexpected RangeFeedEvent variant %v", t)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	return g.Wait()
}

type byValueTimestamp []roachpb.KeyValue

func (b byValueTimestamp) Len() int      { return len(b) }
func (b byValueTimestamp) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b byValueTimestamp) Less(i, j int) bool {
	return b[i].Value.Timestamp.Less(b[j].Value.Timestamp)
}

func allRangeDescriptors(ctx context.Context, txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	rows, err := txn.Scan(ctx, keys.Meta2Prefix, keys.MetaMax, 0)
	if err != nil {
		return nil, err
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

// clusterNodeCount returns the approximate number of nodes in the cluster.
func clusterNodeCount(g *gossip.Gossip) int {
	var nodes int
	_ = g.IterateInfos(gossip.KeyNodeIDPrefix, func(_ string, _ gossip.Info) error {
		nodes++
		return nil
	})
	return nodes
}
