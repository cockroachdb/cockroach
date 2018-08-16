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

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/intervalccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
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
	targets  map[sqlbase.ID]string
	buf      *buffer

	highWater hlc.Timestamp
}

func makePoller(
	execCfg *sql.ExecutorConfig,
	details jobspb.ChangefeedDetails,
	spans []roachpb.Span,
	startTime hlc.Timestamp,
	buf *buffer,
) *poller {
	return &poller{
		settings:  execCfg.Settings,
		db:        execCfg.DB,
		clock:     execCfg.Clock,
		gossip:    execCfg.Gossip,
		highWater: startTime,
		spans:     spans,
		targets:   details.Targets,
		buf:       buf,
	}
}

func fetchSpansForTargets(
	ctx context.Context, db *client.DB, targets map[sqlbase.ID]string, ts hlc.Timestamp,
) ([]roachpb.Span, error) {
	var spans []roachpb.Span
	err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		spans = nil
		if ts != (hlc.Timestamp{}) {
			txn.SetFixedTimestamp(ctx, ts)
		}
		// Note that all targets are currently guaranteed to be tables.
		for tableID, origName := range targets {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
			if err != nil {
				if errors.Cause(err) == sqlbase.ErrDescriptorNotFound {
					return errors.Errorf(`"%s" was dropped or truncated`, origName)
				}
				return err
			}
			if tableDesc.State == sqlbase.TableDescriptor_DROP {
				return errors.Errorf(`"%s" was dropped or truncated`, origName)
			}
			if tableDesc.Name != origName {
				return errors.Errorf(`"%s" was renamed to "%s"`, origName, tableDesc.Name)
			}
			if err := validateChangefeedTable(tableDesc); err != nil {
				return err
			}
			spans = append(spans, tableDesc.PrimaryIndexSpan())
		}
		return nil
	})
	return spans, err
}

func equalSpanSets(a, b roachpb.Spans) bool {
	if len(a) != len(b) {
		return false
	}
	a = append(roachpb.Spans(nil), a...)
	b = append(roachpb.Spans(nil), b...)
	sort.Sort(a)
	sort.Sort(b)
	for i := range a {
		if !a[i].EqualValue(b[i]) {
			return false
		}
	}
	return true
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

		newSpans, err := fetchSpansForTargets(ctx, p.db, p.targets, nextHighWater)
		if err != nil {
			return err
		}
		if !equalSpanSets(p.spans, newSpans) {
			// The SpanFrontier at the end of this changefeed flow currently
			// depends on the set of tracked spans being static. We may have to
			// support it changing eventually, but for now we don't, so error
			// defensively.
			return errors.Errorf(`the set of tracked spans changed: %v to %v`, p.spans, newSpans)
		}

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
				}
				if req.StartTime == (hlc.Timestamp{}) {
					req.MVCCFilter = roachpb.MVCCFilter_Latest
				}
				res, pErr := client.SendWrappedWith(ctx, sender, header, req)
				finished := atomic.AddInt64(&atomicFinished, 1)
				if log.V(2) {
					log.Infof(ctx, `finished ExportRequest [%s,%s) %d of %d`,
						span.Key, span.EndKey, finished, len(requests))
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
				return p.buf.AddResolved(ctx, span, nextHighWater)
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
	for k := range g.GetInfoStatus().Infos {
		if gossip.IsNodeIDKey(k) {
			nodes++
		}
	}
	return nodes
}
