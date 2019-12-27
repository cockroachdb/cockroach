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
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/covering"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// poller uses ExportRequest with the `ReturnSST` to repeatedly fetch every kv
// that changed between a set of timestamps and insert them into a buffer.
//
// Each poll (ie set of ExportRequests) are rate limited to be no more often
// than the `changefeed.experimental_poll_interval` setting.
type poller struct {
	settings  *cluster.Settings
	db        *client.DB
	clock     *hlc.Clock
	gossip    *gossip.Gossip
	spans     []roachpb.Span
	details   jobspb.ChangefeedDetails
	buf       *buffer
	tableHist *tableHistory
	leaseMgr  *sql.LeaseManager
	metrics   *Metrics
	mm        *mon.BytesMonitor

	mu struct {
		syncutil.Mutex
		// highWater timestamp for exports processed by this poller so far.
		highWater hlc.Timestamp
		// scanBoundaries represent timestamps where the changefeed output process
		// should pause and output a scan of *all keys* of the watched spans at the
		// given timestamp. There are currently two situations where this occurs:
		// the initial scan of the table when starting a new Changefeed, and when
		// a backfilling schema change is marked as completed. This collection must
		// be kept in sorted order (by timestamp ascending).
		scanBoundaries []hlc.Timestamp
		// previousTableVersion is a map from tableID to the most recent version
		// of the table descriptor seen by the poller. This is needed to determine
		// when a backilling mutation has successfully completed - this can only
		// be determining by comparing a version to the previous version.
		previousTableVersion map[sqlbase.ID]*sqlbase.TableDescriptor
	}
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
	leaseMgr *sql.LeaseManager,
	metrics *Metrics,
	mm *mon.BytesMonitor,
) *poller {
	p := &poller{
		settings: settings,
		db:       db,
		clock:    clock,
		gossip:   gossip,

		spans:    spans,
		details:  details,
		buf:      buf,
		leaseMgr: leaseMgr,
		metrics:  metrics,
		mm:       mm,
	}
	p.mu.previousTableVersion = make(map[sqlbase.ID]*sqlbase.TableDescriptor)
	// If no highWater is specified, set the highwater to the statement time
	// and add a scanBoundary at the statement time to trigger an immediate output
	// of the full table.
	if highWater == (hlc.Timestamp{}) {
		p.mu.highWater = details.StatementTime
		p.mu.scanBoundaries = append(p.mu.scanBoundaries, details.StatementTime)
	} else {
		p.mu.highWater = highWater
	}
	p.tableHist = makeTableHistory(p.validateTable, highWater)

	return p
}

// RunUsingRangeFeeds performs the same task as the normal Run method, but uses
// the experimental Rangefeed system to capture changes rather than the
// poll-and-export method.  Note
func (p *poller) RunUsingRangefeeds(ctx context.Context) error {
	// Fetch the table descs as of the initial highWater and prime the table
	// history with them. This addresses #41694 where we'd skip the rest of a
	// backfill if the changefeed was paused/unpaused during it. The bug was that
	// the changefeed wouldn't notice the table descriptor had changed (and thus
	// we were in the backfill state) when it restarted.
	if err := p.primeInitialTableDescs(ctx); err != nil {
		return err
	}

	// Start polling tablehistory, which must be done concurrently with
	// the individual rangefeed routines.
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(p.pollTableHistory)
	g.GoCtx(p.rangefeedImpl)
	return g.Wait()
}

func (p *poller) primeInitialTableDescs(ctx context.Context) error {
	p.mu.Lock()
	initialTableDescTs := p.mu.highWater
	p.mu.Unlock()
	var initialDescs []*sqlbase.TableDescriptor
	initialTableDescsFn := func(ctx context.Context, txn *client.Txn) error {
		initialDescs = initialDescs[:0]
		txn.SetFixedTimestamp(ctx, initialTableDescTs)
		// Note that all targets are currently guaranteed to be tables.
		for tableID := range p.details.Targets {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
			if err != nil {
				return err
			}
			initialDescs = append(initialDescs, tableDesc)
		}
		return nil
	}
	if err := p.db.Txn(ctx, initialTableDescsFn); err != nil {
		return err
	}
	return p.tableHist.IngestDescriptors(ctx, hlc.Timestamp{}, initialTableDescTs, initialDescs)
}

var errBoundaryReached = errors.New("scan boundary reached")

func (p *poller) rangefeedImpl(ctx context.Context) error {
	for i := 0; ; i++ {
		if err := p.rangefeedImplIter(ctx, i); err != nil {
			return err
		}
	}
}

func (p *poller) rangefeedImplIter(ctx context.Context, i int) error {
	// Determine whether to request the previous value of each update from
	// RangeFeed based on whether the `diff` option is specified.
	_, withDiff := p.details.Opts[optDiff]

	p.mu.Lock()
	lastHighwater := p.mu.highWater
	p.mu.Unlock()
	if err := p.tableHist.WaitForTS(ctx, lastHighwater); err != nil {
		return err
	}

	spans, err := getSpansToProcess(ctx, p.db, p.spans)
	if err != nil {
		return err
	}

	// Perform a full scan if necessary - either an initial scan or a backfill
	// Full scans are still performed using an Export operation.
	initialScan := i == 0
	backfillWithDiff := !initialScan && withDiff
	var scanTime hlc.Timestamp
	p.mu.Lock()
	if len(p.mu.scanBoundaries) > 0 && p.mu.scanBoundaries[0].Equal(p.mu.highWater) {
		// Perform a full scan of the latest value of all keys as of the
		// boundary timestamp and consume the boundary.
		scanTime = p.mu.scanBoundaries[0]
		p.mu.scanBoundaries = p.mu.scanBoundaries[1:]
	}
	p.mu.Unlock()
	if scanTime != (hlc.Timestamp{}) {
		// TODO(dan): Now that we no longer have the poller, we should stop using
		// ExportRequest and start using normal Scans.
		if err := p.exportSpansParallel(ctx, spans, scanTime, backfillWithDiff); err != nil {
			return err
		}
	}

	// Start rangefeeds, exit polling if we hit a resolved timestamp beyond
	// the next scan boundary.

	// TODO(nvanbenschoten): This is horrible.
	sender := p.db.NonTransactionalSender()
	ds := sender.(*client.CrossRangeTxnWrapperSender).Wrapped().(*kv.DistSender)
	g := ctxgroup.WithContext(ctx)
	eventC := make(chan *roachpb.RangeFeedEvent, 128)

	// To avoid blocking raft, RangeFeed puts all entries in a server side
	// buffer. But to keep things simple, it's a small fixed-sized buffer. This
	// means we need to ingest everything we get back as quickly as possible, so
	// we throw it in a buffer here to pick up the slack between RangeFeed and
	// the sink.
	//
	// TODO(dan): Right now, there are two buffers in the changefeed flow when
	// using RangeFeeds, one here and the usual one between the poller and the
	// rest of the changefeed (he latter of which is implemented with an
	// unbuffered channel, and so doesn't actually buffer). Ideally, we'd have
	// one, but the structure of the poller code right now makes this hard.
	// Specifically, when a schema change happens, we need a barrier where we
	// flush out every change before the schema change timestamp before we start
	// emitting any changes from after the schema change. The poller's
	// `tableHist` is responsible for detecting and enforcing these (they queue
	// up in `p.scanBoundaries`), but the after-poller buffer doesn't have
	// access to any of this state. A cleanup is in order.
	memBuf := makeMemBuffer(p.mm.MakeBoundAccount(), p.metrics)
	defer memBuf.Close(ctx)

	// Maintain a local spanfrontier to tell when all the component rangefeeds
	// being watched have reached the Scan boundary.
	// TODO(mrtracy): The alternative to this would be to maintain two
	// goroutines for each span (the current arrangement is one goroutine per
	// span and one multiplexing goroutine that outputs to the buffer). This
	// alternative would allow us to stop the individual rangefeeds earlier and
	// avoid the need for a span frontier, but would also introduce a different
	// contention pattern and use additional goroutines. it's not clear which
	// solution is best without targeted performance testing, so we're choosing
	// the faster-to-implement solution for now.
	frontier := makeSpanFrontier(spans...)

	rangeFeedStartTS := lastHighwater
	for _, span := range p.spans {
		span := span
		frontier.Forward(span, rangeFeedStartTS)
		g.GoCtx(func(ctx context.Context) error {
			return ds.RangeFeed(ctx, span, rangeFeedStartTS, withDiff, eventC)
		})
	}
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case e := <-eventC:
				switch t := e.GetValue().(type) {
				case *roachpb.RangeFeedValue:
					kv := roachpb.KeyValue{Key: t.Key, Value: t.Value}
					var prevVal roachpb.Value
					if withDiff {
						prevVal = t.PrevValue
					}
					if err := memBuf.AddKV(ctx, kv, prevVal); err != nil {
						return err
					}
				case *roachpb.RangeFeedCheckpoint:
					if !t.ResolvedTS.IsEmpty() && t.ResolvedTS.Less(rangeFeedStartTS) {
						// RangeFeed happily forwards any closed timestamps it receives as
						// soon as there are no outstanding intents under them.
						// Changefeeds don't care about these at all, so throw them out.
						continue
					}
					if err := memBuf.AddResolved(ctx, t.Span, t.ResolvedTS); err != nil {
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
	g.GoCtx(func(ctx context.Context) error {
		for {
			e, err := memBuf.Get(ctx)
			if err != nil {
				return err
			}
			if e.kv.Key != nil {
				if err := p.tableHist.WaitForTS(ctx, e.kv.Value.Timestamp); err != nil {
					return err
				}
				pastBoundary := false
				p.mu.Lock()
				if len(p.mu.scanBoundaries) > 0 && p.mu.scanBoundaries[0].Less(e.kv.Value.Timestamp) {
					// Ignore feed results beyond the next boundary; they will be retrieved when
					// the feeds are restarted after the scan.
					pastBoundary = true
				}
				p.mu.Unlock()
				if pastBoundary {
					continue
				}
				if err := p.buf.AddKV(ctx, e.kv, e.prevVal, e.backfillTimestamp); err != nil {
					return err
				}
			} else if e.resolved != nil {
				resolvedTS := e.resolved.Timestamp
				boundaryBreak := false
				// Make sure scan boundaries less than or equal to `resolvedTS` were
				// added to the `scanBoundaries` list before proceeding.
				if err := p.tableHist.WaitForTS(ctx, resolvedTS); err != nil {
					return err
				}
				p.mu.Lock()
				if len(p.mu.scanBoundaries) > 0 && p.mu.scanBoundaries[0].LessEq(resolvedTS) {
					boundaryBreak = true
					resolvedTS = p.mu.scanBoundaries[0]
				}
				p.mu.Unlock()
				if boundaryBreak {
					// A boundary here means we're about to do a full scan (backfill)
					// at this timestamp, so at the changefeed level the boundary
					// itself is not resolved. Skip emitting this resolved timestamp
					// because we want to trigger the scan first before resolving its
					// scan boundary timestamp.
					resolvedTS = resolvedTS.Prev()
					frontier.Forward(e.resolved.Span, resolvedTS)
					if frontier.Frontier() == resolvedTS {
						// All component rangefeeds are now at the boundary.
						// Break out of the ctxgroup by returning a sentinel error.
						return errBoundaryReached
					}
				} else {
					if err := p.buf.AddResolved(ctx, e.resolved.Span, resolvedTS); err != nil {
						return err
					}
				}
			}
		}
	})
	// TODO(mrtracy): We are currently tearing down the entire rangefeed set in
	// order to perform a scan; however, given that we have an intermediate
	// buffer, its seems that we could do this without having to destroy and
	// recreate the rangefeeds.
	if err := g.Wait(); err != nil && err != errBoundaryReached {
		return err
	}

	p.mu.Lock()
	p.mu.highWater = p.mu.scanBoundaries[0]
	p.mu.Unlock()
	return nil
}

func getSpansToProcess(
	ctx context.Context, db *client.DB, targetSpans []roachpb.Span,
) ([]roachpb.Span, error) {
	var ranges []roachpb.RangeDescriptor
	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		ranges, err = allRangeDescriptors(ctx, txn)
		return err
	}); err != nil {
		return nil, errors.Wrapf(err, "fetching range descriptors")
	}

	type spanMarker struct{}
	type rangeMarker struct{}

	var spanCovering covering.Covering
	for _, span := range targetSpans {
		spanCovering = append(spanCovering, covering.Range{
			Start:   []byte(span.Key),
			End:     []byte(span.EndKey),
			Payload: spanMarker{},
		})
	}

	var rangeCovering covering.Covering
	for _, rangeDesc := range ranges {
		rangeCovering = append(rangeCovering, covering.Range{
			Start:   []byte(rangeDesc.StartKey),
			End:     []byte(rangeDesc.EndKey),
			Payload: rangeMarker{},
		})
	}

	chunks := covering.OverlapCoveringMerge(
		[]covering.Covering{spanCovering, rangeCovering},
	)

	var requests []roachpb.Span
	for _, chunk := range chunks {
		if _, ok := chunk.Payload.([]interface{})[0].(spanMarker); !ok {
			continue
		}
		requests = append(requests, roachpb.Span{Key: chunk.Start, EndKey: chunk.End})
	}
	return requests, nil
}

func (p *poller) exportSpansParallel(
	ctx context.Context, spans []roachpb.Span, ts hlc.Timestamp, withDiff bool,
) error {
	// Export requests for the various watched spans are executed in parallel,
	// with a semaphore-enforced limit based on a cluster setting.
	maxConcurrentExports := clusterNodeCount(p.gossip) *
		int(storage.ExportRequestsLimit.Get(&p.settings.SV))
	exportsSem := make(chan struct{}, maxConcurrentExports)
	g := ctxgroup.WithContext(ctx)

	// atomicFinished is used only to enhance debugging messages.
	var atomicFinished int64

	for _, span := range spans {
		span := span

		// Wait for our semaphore.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case exportsSem <- struct{}{}:
		}

		g.GoCtx(func(ctx context.Context) error {
			defer func() { <-exportsSem }()

			err := p.exportSpan(ctx, span, ts, withDiff)
			finished := atomic.AddInt64(&atomicFinished, 1)
			if log.V(2) {
				log.Infof(ctx, `exported %d of %d: %v`, finished, len(spans), err)
			}
			if err != nil {
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func (p *poller) exportSpan(
	ctx context.Context, span roachpb.Span, ts hlc.Timestamp, withDiff bool,
) error {
	sender := p.db.NonTransactionalSender()
	if log.V(2) {
		log.Infof(ctx, `sending ExportRequest %s at %s`, span, ts)
	}

	header := roachpb.Header{Timestamp: ts}
	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(span),
		MVCCFilter:    roachpb.MVCCFilter_Latest,
		ReturnSST:     true,
		OmitChecksum:  true,
	}

	stopwatchStart := timeutil.Now()
	exported, pErr := client.SendWrappedWith(ctx, sender, header, req)
	exportDuration := timeutil.Since(stopwatchStart)
	if log.V(2) {
		log.Infof(ctx, `finished ExportRequest %s at %s took %s`,
			span, ts.AsOfSystemTime(), exportDuration)
	}
	slowExportThreshold := 10 * changefeedPollInterval.Get(&p.settings.SV)
	if exportDuration > slowExportThreshold {
		log.Infof(ctx, "finished ExportRequest %s at %s took %s behind by %s",
			span, ts, exportDuration, timeutil.Since(ts.GoTime()))
	}

	if pErr != nil {
		err := pErr.GoError()
		return errors.Wrapf(err, `fetching changes for %s`, span)
	}
	p.metrics.PollRequestNanosHist.RecordValue(exportDuration.Nanoseconds())

	// When outputting a full scan, we want to use the schema at the scan
	// timestamp, not the schema at the value timestamp.
	schemaTimestamp := ts
	stopwatchStart = timeutil.Now()
	for _, file := range exported.(*roachpb.ExportResponse).Files {
		if err := p.slurpSST(ctx, file.SST, schemaTimestamp, withDiff); err != nil {
			return err
		}
	}
	if err := p.buf.AddResolved(ctx, span, ts); err != nil {
		return err
	}

	if log.V(2) {
		log.Infof(ctx, `finished buffering %s took %s`, span, timeutil.Since(stopwatchStart))
	}
	return nil
}

func (p *poller) updateTableHistory(ctx context.Context, endTS hlc.Timestamp) error {
	startTS := p.tableHist.HighWater()
	if endTS.LessEq(startTS) {
		return nil
	}
	descs, err := fetchTableDescriptorVersions(ctx, p.db, startTS, endTS, p.details.Targets)
	if err != nil {
		return err
	}
	return p.tableHist.IngestDescriptors(ctx, startTS, endTS, descs)
}

func (p *poller) pollTableHistory(ctx context.Context) error {
	for {
		if err := p.updateTableHistory(ctx, p.clock.Now()); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(changefeedPollInterval.Get(&p.settings.SV)):
		}
	}
}

// slurpSST iterates an encoded sst and inserts the contained kvs into the
// buffer.
func (p *poller) slurpSST(
	ctx context.Context, sst []byte, schemaTimestamp hlc.Timestamp, withDiff bool,
) error {
	var previousKey roachpb.Key
	var kvs []roachpb.KeyValue
	slurpKVs := func() error {
		sort.Sort(byValueTimestamp(kvs))
		for _, kv := range kvs {
			var prevVal roachpb.Value
			if withDiff {
				// Include the same value for the "before" and "after" KV, but
				// interpret them at different timestamp. Specifically, interpret
				// the "before" KV at the timestamp immediately before the schema
				// change. This is handled in kvsToRows.
				prevVal = kv.Value
			}
			if err := p.buf.AddKV(ctx, kv, prevVal, schemaTimestamp); err != nil {
				return err
			}
		}
		previousKey = previousKey[:0]
		kvs = kvs[:0]
		return nil
	}

	var scratch bufalloc.ByteAllocator
	it, err := engine.NewMemSSTIterator(sst, false /* verify */)
	if err != nil {
		return err
	}
	defer it.Close()
	for it.SeekGE(engine.NilKey); ; it.Next() {
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
			return nil, errors.NewAssertionErrorWithWrappedErrf(err,
				"%s: unable to unmarshal range descriptor", row.Key)
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

func (p *poller) validateTable(ctx context.Context, desc *sqlbase.TableDescriptor) error {
	if err := validateChangefeedTable(p.details.Targets, desc); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if lastVersion, ok := p.mu.previousTableVersion[desc.ID]; ok {
		if desc.ModificationTime.Less(lastVersion.ModificationTime) {
			return nil
		}
		if shouldAddScanBoundary(lastVersion, desc) {
			boundaryTime := desc.GetModificationTime()
			// Only mutations that happened after the changefeed started are
			// interesting here.
			if p.details.StatementTime.Less(boundaryTime) {
				if boundaryTime.Less(p.mu.highWater) {
					return errors.AssertionFailedf(
						"error: detected table ID %d backfill completed at %s "+
							"earlier than highwater timestamp %s",
						errors.Safe(desc.ID),
						errors.Safe(boundaryTime),
						errors.Safe(p.mu.highWater),
					)
				}
				p.mu.scanBoundaries = append(p.mu.scanBoundaries, boundaryTime)
				sort.Slice(p.mu.scanBoundaries, func(i, j int) bool {
					return p.mu.scanBoundaries[i].Less(p.mu.scanBoundaries[j])
				})
				// To avoid race conditions with the lease manager, at this point we force
				// the manager to acquire the freshest descriptor of this table from the
				// store. In normal operation, the lease manager returns the newest
				// descriptor it knows about for the timestamp, assuming it's still
				// allowed; without this explicit load, the lease manager might therefore
				// return the previous version of the table, which is still technically
				// allowed by the schema change system.
				if err := p.leaseMgr.AcquireFreshestFromStore(ctx, desc.ID); err != nil {
					return err
				}
			}
		}
	}
	p.mu.previousTableVersion[desc.ID] = desc
	return nil
}

func shouldAddScanBoundary(
	lastVersion *sqlbase.TableDescriptor, desc *sqlbase.TableDescriptor,
) (res bool) {
	return newColumnBackfillComplete(lastVersion, desc) ||
		hasNewColumnDropBackfillMutation(lastVersion, desc)
}

func hasNewColumnDropBackfillMutation(oldDesc, newDesc *sqlbase.TableDescriptor) (res bool) {
	dropMutationExists := func(desc *sqlbase.TableDescriptor) bool {
		for _, m := range desc.Mutations {
			if m.Direction == sqlbase.DescriptorMutation_DROP &&
				m.State == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
				return true
			}
		}
		return false
	}
	// Make sure that the old descriptor *doesn't* have the same mutation to avoid adding
	// the same scan boundary more than once.
	return !dropMutationExists(oldDesc) && dropMutationExists(newDesc)
}

func newColumnBackfillComplete(oldDesc, newDesc *sqlbase.TableDescriptor) (res bool) {
	return len(oldDesc.Columns) < len(newDesc.Columns) &&
		oldDesc.HasColumnBackfillMutation() && !newDesc.HasColumnBackfillMutation()
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
