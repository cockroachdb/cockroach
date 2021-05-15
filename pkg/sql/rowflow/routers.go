// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// Routers are used by processors to direct outgoing rows to (potentially)
// multiple streams; see docs/RFCS/distributed_sql.md

package rowflow

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/rowcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

type router interface {
	execinfra.RowReceiver
	flowinfra.Startable
	init(ctx context.Context, flowCtx *execinfra.FlowCtx, types []*types.T)
}

// makeRouter creates a router. The router's init must be called before the
// router can be started.
//
// Pass-through routers are not supported; the higher layer is expected to elide
// them.
func makeRouter(
	spec *execinfrapb.OutputRouterSpec, streams []execinfra.RowReceiver,
) (router, error) {
	if len(streams) == 0 {
		return nil, errors.Errorf("no streams in router")
	}

	var rb routerBase
	rb.setupStreams(spec, streams)

	switch spec.Type {
	case execinfrapb.OutputRouterSpec_BY_HASH:
		return makeHashRouter(rb, spec.HashColumns)

	case execinfrapb.OutputRouterSpec_MIRROR:
		return makeMirrorRouter(rb)

	case execinfrapb.OutputRouterSpec_BY_RANGE:
		return makeRangeRouter(rb, spec.RangeRouterSpec)

	default:
		return nil, errors.Errorf("router type %s not supported", spec.Type)
	}
}

const routerRowBufSize = execinfra.RowChannelBufSize

// routerOutput is the data associated with one router consumer.
type routerOutput struct {
	stream   execinfra.RowReceiver
	streamID execinfrapb.StreamID
	mu       struct {
		syncutil.Mutex
		// cond is signaled whenever the main router routine adds a metadata item, a
		// row, or sets producerDone.
		cond         *sync.Cond
		streamStatus execinfra.ConsumerStatus

		metadataBuf []*execinfrapb.ProducerMetadata
		// The "level 1" row buffer is used first, to avoid going through the row
		// container if we don't need to buffer many rows. The buffer is a circular
		// FIFO queue, with rowBufLen elements and the left-most (oldest) element at
		// rowBufLeft.
		rowBuf                [routerRowBufSize]rowenc.EncDatumRow
		rowBufLeft, rowBufLen uint32

		// The "level 2" rowContainer is used when we need to buffer more rows than
		// rowBuf allows. The row container always contains rows "older" than those
		// in rowBuf. The oldest rows are at the beginning of the row container.
		rowContainer rowcontainer.DiskBackedRowContainer
		producerDone bool
	}
	// TODO(radu): add padding of size sys.CacheLineSize to ensure there is no
	// false-sharing?

	stats execinfrapb.ComponentStats

	// memoryMonitor and diskMonitor are mu.rowContainer's monitors.
	memoryMonitor, diskMonitor *mon.BytesMonitor

	rowAlloc         rowenc.EncDatumRowAlloc
	rowBufToPushFrom [routerRowBufSize]rowenc.EncDatumRow
	// rowBufToPushFromMon and rowBufToPushFromAcc are the memory accounting
	// infrastructure of rowBufToPushFrom.
	rowBufToPushFromMon *mon.BytesMonitor
	rowBufToPushFromAcc *mon.BoundAccount
	// rowBufToPushFromRowSize stores the size of the row that we have
	// accounted for when adding it to rowBufToPushFrom buffer in ith position.
	rowBufToPushFromRowSize [routerRowBufSize]int64
}

func (ro *routerOutput) addMetadataLocked(meta *execinfrapb.ProducerMetadata) {
	// We don't need any fancy buffering because normally there is not a lot of
	// metadata being passed around.
	ro.mu.metadataBuf = append(ro.mu.metadataBuf, meta)
}

// addRowLocked adds a row to rowBuf (potentially evicting the oldest row into
// rowContainer).
func (ro *routerOutput) addRowLocked(ctx context.Context, row rowenc.EncDatumRow) error {
	if ro.mu.streamStatus != execinfra.NeedMoreRows {
		// The consumer doesn't want more rows; drop the row.
		return nil
	}
	if ro.mu.rowBufLen == routerRowBufSize {
		// Take out the oldest row in rowBuf and put it in rowContainer.
		evictedRow := ro.mu.rowBuf[ro.mu.rowBufLeft]
		if err := ro.mu.rowContainer.AddRow(ctx, evictedRow); err != nil {
			return err
		}

		ro.mu.rowBufLeft = (ro.mu.rowBufLeft + 1) % routerRowBufSize
		ro.mu.rowBufLen--
	}
	ro.mu.rowBuf[(ro.mu.rowBufLeft+ro.mu.rowBufLen)%routerRowBufSize] = row
	ro.mu.rowBufLen++
	return nil
}

func (ro *routerOutput) popRowsLocked(ctx context.Context) ([]rowenc.EncDatumRow, error) {
	n := 0
	// addToRowBufToPushFrom adds row to nth position in rowBufToPushFrom. row
	// *must* be safe from further modifications.
	addToRowBufToPushFrom := func(row rowenc.EncDatumRow) error {
		// We're reusing the same rowBufToPushFrom slice, so we can only
		// release the memory under the "old" row once we overwrite it in
		// rowBufToPushFrom which we're about to do for rowBufToPushFrom[n].
		rowSize := int64(row.Size())
		delta := rowSize - ro.rowBufToPushFromRowSize[n]
		ro.rowBufToPushFromRowSize[n] = rowSize
		if err := ro.rowBufToPushFromAcc.Grow(ctx, delta); err != nil {
			return err
		}
		ro.rowBufToPushFrom[n] = row
		return nil
	}
	// First try to get rows from the row container.
	if ro.mu.rowContainer.Len() > 0 {
		if err := func() error {
			i := ro.mu.rowContainer.NewFinalIterator(ctx)
			defer i.Close()
			for i.Rewind(); n < len(ro.rowBufToPushFrom); i.Next() {
				if ok, err := i.Valid(); err != nil {
					return err
				} else if !ok {
					break
				}
				row, err := i.Row()
				if err != nil {
					return err
				}
				if err = addToRowBufToPushFrom(ro.rowAlloc.CopyRow(row)); err != nil {
					return err
				}
				n++
			}
			return nil
		}(); err != nil {
			return nil, err
		}
	}

	// If the row container is empty, get more rows from the row buffer.
	for ; n < len(ro.rowBufToPushFrom) && ro.mu.rowBufLen > 0; n++ {
		if err := addToRowBufToPushFrom(ro.mu.rowBuf[ro.mu.rowBufLeft]); err != nil {
			return nil, err
		}
		ro.mu.rowBufLeft = (ro.mu.rowBufLeft + 1) % routerRowBufSize
		ro.mu.rowBufLen--
	}
	return ro.rowBufToPushFrom[:n], nil
}

// See the comment for routerBase.semaphoreCount.
const semaphorePeriod = 8

type routerBase struct {
	types []*types.T

	outputs []routerOutput

	// How many of streams are not in the DrainRequested or ConsumerClosed state.
	numNonDrainingStreams int32

	// aggregatedStatus is an atomic that maintains a unified view across all
	// streamStatus'es.  Namely, if at least one of them is NeedMoreRows, this
	// will be NeedMoreRows. If all of them are ConsumerClosed, this will
	// (eventually) be ConsumerClosed. Otherwise, this will be DrainRequested.
	aggregatedStatus uint32

	// We use a semaphore of size len(outputs) and acquire it whenever we Push
	// to each stream as well as in the router's main Push routine. This ensures
	// that if all outputs are blocked, the main router routine blocks as well
	// (preventing runaway buffering if the source is faster than the consumers).
	semaphore chan struct{}

	// To reduce synchronization overhead, we only acquire the semaphore once for
	// every semaphorePeriod rows. This count keeps track of how many rows we
	// saw since the last time we took the semaphore.
	semaphoreCount int32

	statsCollectionEnabled bool
}

func (rb *routerBase) aggStatus() execinfra.ConsumerStatus {
	return execinfra.ConsumerStatus(atomic.LoadUint32(&rb.aggregatedStatus))
}

func (rb *routerBase) setupStreams(
	spec *execinfrapb.OutputRouterSpec, streams []execinfra.RowReceiver,
) {
	rb.numNonDrainingStreams = int32(len(streams))
	n := len(streams)
	if spec.DisableBuffering {
		// By starting the semaphore at 1, the producer is blocked whenever one of
		// the streams are blocked.
		n = 1
		// TODO(radu): instead of disabling buffering this way, we should short-circuit
		// the entire router implementation and push directly to the output stream
	}
	rb.semaphore = make(chan struct{}, n)
	rb.outputs = make([]routerOutput, len(streams))
	for i := range rb.outputs {
		ro := &rb.outputs[i]
		ro.stream = streams[i]
		ro.streamID = spec.Streams[i].StreamID
		ro.mu.cond = sync.NewCond(&ro.mu.Mutex)
		ro.mu.streamStatus = execinfra.NeedMoreRows
	}
}

// init must be called after setupStreams but before Start.
func (rb *routerBase) init(ctx context.Context, flowCtx *execinfra.FlowCtx, types []*types.T) {
	// Check if we're recording stats.
	if s := tracing.SpanFromContext(ctx); s != nil && s.IsVerbose() {
		rb.statsCollectionEnabled = true
	}

	rb.types = types
	for i := range rb.outputs {
		// This method must be called before we Start() so we don't need
		// to take the mutex.
		evalCtx := flowCtx.NewEvalCtx()
		rb.outputs[i].memoryMonitor = execinfra.NewLimitedMonitor(
			ctx, evalCtx.Mon, flowCtx,
			fmt.Sprintf("router-limited-%d", rb.outputs[i].streamID),
		)
		rb.outputs[i].diskMonitor = execinfra.NewMonitor(
			ctx, flowCtx.DiskMonitor,
			fmt.Sprintf("router-disk-%d", rb.outputs[i].streamID),
		)
		// Note that the monitor is an unlimited one since we don't know how
		// to fallback to disk if a memory budget error is encountered when
		// we're popping rows from the row container into the row buffer.
		rb.outputs[i].rowBufToPushFromMon = execinfra.NewMonitor(
			ctx, evalCtx.Mon, fmt.Sprintf("router-unlimited-%d", rb.outputs[i].streamID),
		)
		memAcc := rb.outputs[i].rowBufToPushFromMon.MakeBoundAccount()
		rb.outputs[i].rowBufToPushFromAcc = &memAcc

		rb.outputs[i].mu.rowContainer.Init(
			nil, /* ordering */
			types,
			evalCtx,
			flowCtx.Cfg.TempStorage,
			rb.outputs[i].memoryMonitor,
			rb.outputs[i].diskMonitor,
		)

		// Initialize any outboxes.
		if o, ok := rb.outputs[i].stream.(*flowinfra.Outbox); ok {
			o.Init(types)
		}
	}
}

// Start must be called after init.
func (rb *routerBase) Start(ctx context.Context, wg *sync.WaitGroup, _ context.CancelFunc) {
	wg.Add(len(rb.outputs))
	for i := range rb.outputs {
		go func(ctx context.Context, rb *routerBase, ro *routerOutput, wg *sync.WaitGroup) {
			var span *tracing.Span
			if rb.statsCollectionEnabled {
				ctx, span = execinfra.ProcessorSpan(ctx, "router output")
				span.SetTag(execinfrapb.StreamIDTagKey, ro.streamID)
				ro.stats.Inputs = make([]execinfrapb.InputStats, 1)
			}

			drain := false
			streamStatus := execinfra.NeedMoreRows
			ro.mu.Lock()
			for {
				// Send any metadata that has been buffered. Note that we are not
				// maintaining the relative ordering between metadata items and rows
				// (but it doesn't matter).
				if len(ro.mu.metadataBuf) > 0 {
					m := ro.mu.metadataBuf[0]
					// Reset the value so any objects it refers to can be garbage
					// collected.
					ro.mu.metadataBuf[0] = nil
					ro.mu.metadataBuf = ro.mu.metadataBuf[1:]

					ro.mu.Unlock()

					rb.semaphore <- struct{}{}
					status := ro.stream.Push(nil /*row*/, m)
					<-rb.semaphore

					rb.updateStreamState(&streamStatus, status)
					ro.mu.Lock()
					ro.mu.streamStatus = streamStatus
					continue
				}

				if !drain {
					// Send any rows that have been buffered. We grab multiple rows at a
					// time to reduce contention.
					if rows, err := ro.popRowsLocked(ctx); err != nil {
						ro.mu.Unlock()
						rb.fwdMetadata(&execinfrapb.ProducerMetadata{Err: err})
						ro.mu.Lock()
						atomic.StoreUint32(&rb.aggregatedStatus, uint32(execinfra.DrainRequested))
						drain = true
						continue
					} else if len(rows) > 0 {
						ro.mu.Unlock()
						rb.semaphore <- struct{}{}
						for _, row := range rows {
							status := ro.stream.Push(row, nil)
							rb.updateStreamState(&streamStatus, status)
						}
						<-rb.semaphore
						if rb.statsCollectionEnabled {
							ro.stats.Inputs[0].NumTuples.Add(int64(len(rows)))
						}
						ro.mu.Lock()
						ro.mu.streamStatus = streamStatus
						continue
					}
				}

				// No rows or metadata buffered; see if the producer is done.
				if ro.mu.producerDone {
					if rb.statsCollectionEnabled {
						ro.stats.Exec.MaxAllocatedMem.Set(uint64(ro.memoryMonitor.MaximumBytes()))
						ro.stats.Exec.MaxAllocatedDisk.Set(uint64(ro.diskMonitor.MaximumBytes()))
						span.RecordStructured(&ro.stats)
						span.Finish()
						if trace := execinfra.GetTraceData(ctx); trace != nil {
							ro.mu.Unlock()
							rb.semaphore <- struct{}{}
							status := ro.stream.Push(nil, &execinfrapb.ProducerMetadata{TraceData: trace})
							rb.updateStreamState(&streamStatus, status)
							<-rb.semaphore
							ro.mu.Lock()
						}
					}
					ro.stream.ProducerDone()
					break
				}

				// Nothing to do; wait.
				ro.mu.cond.Wait()
			}
			ro.mu.rowContainer.Close(ctx)
			ro.mu.Unlock()

			ro.rowBufToPushFromAcc.Close(ctx)
			ro.memoryMonitor.Stop(ctx)
			ro.diskMonitor.Stop(ctx)
			ro.rowBufToPushFromMon.Stop(ctx)

			wg.Done()
		}(ctx, rb, &rb.outputs[i], wg)
	}
}

// ProducerDone is part of the RowReceiver interface.
func (rb *routerBase) ProducerDone() {
	for i := range rb.outputs {
		o := &rb.outputs[i]
		o.mu.Lock()
		o.mu.producerDone = true
		o.mu.Unlock()
		o.mu.cond.Signal()
	}
}

// updateStreamState updates the status of one stream and, if this was the last
// open stream, it also updates rb.aggregatedStatus.
func (rb *routerBase) updateStreamState(
	streamStatus *execinfra.ConsumerStatus, newState execinfra.ConsumerStatus,
) {
	if newState != *streamStatus {
		if *streamStatus == execinfra.NeedMoreRows {
			// A stream state never goes from draining to non-draining, so we can assume
			// that this stream is now draining or closed.
			if atomic.AddInt32(&rb.numNonDrainingStreams, -1) == 0 {
				// Update aggregatedStatus, if the current value is NeedMoreRows.
				atomic.CompareAndSwapUint32(
					&rb.aggregatedStatus,
					uint32(execinfra.NeedMoreRows),
					uint32(execinfra.DrainRequested),
				)
			}
		}
		*streamStatus = newState
	}
}

// fwdMetadata forwards a metadata record to streams that are still accepting
// data. Note that if the metadata record contains an error, it is propagated
// to all non-closed streams whereas all other types of metadata are propagated
// only to the first non-closed stream.
// Note: fwdMetadata should be called without holding the lock.
func (rb *routerBase) fwdMetadata(meta *execinfrapb.ProducerMetadata) {
	if meta == nil {
		log.Fatalf(context.TODO(), "asked to fwd empty metadata")
		return
	}

	rb.semaphore <- struct{}{}
	defer func() {
		<-rb.semaphore
	}()
	if metaErr := meta.Err; metaErr != nil {
		// Forward the error to all non-closed streams.
		if rb.fwdErrMetadata(metaErr) {
			return
		}
	} else {
		// Forward the metadata to the first non-closed stream.
		for i := range rb.outputs {
			ro := &rb.outputs[i]
			ro.mu.Lock()
			if ro.mu.streamStatus != execinfra.ConsumerClosed {
				ro.addMetadataLocked(meta)
				ro.mu.Unlock()
				ro.mu.cond.Signal()
				return
			}
			ro.mu.Unlock()
		}
	}
	// If we got here it means that we couldn't even forward metadata anywhere;
	// all streams are closed.
	atomic.StoreUint32(&rb.aggregatedStatus, uint32(execinfra.ConsumerClosed))
}

// fwdErrMetadata forwards err to all non-closed streams and returns a boolean
// indicating whether it was sent on at least one stream. Note that this method
// assumes that rb.semaphore has been acquired and leaves it up to the caller
// to release it.
func (rb *routerBase) fwdErrMetadata(err error) bool {
	forwarded := false
	for i := range rb.outputs {
		ro := &rb.outputs[i]
		ro.mu.Lock()
		if ro.mu.streamStatus != execinfra.ConsumerClosed {
			meta := &execinfrapb.ProducerMetadata{Err: err}
			ro.addMetadataLocked(meta)
			ro.mu.Unlock()
			ro.mu.cond.Signal()
			forwarded = true
		} else {
			ro.mu.Unlock()
		}
	}
	return forwarded
}

func (rb *routerBase) shouldUseSemaphore() bool {
	rb.semaphoreCount++
	if rb.semaphoreCount >= semaphorePeriod {
		rb.semaphoreCount = 0
		return true
	}
	return false
}

type mirrorRouter struct {
	routerBase
}

type hashRouter struct {
	routerBase

	hashCols []uint32
	buffer   []byte
	alloc    rowenc.DatumAlloc
}

// rangeRouter is a router that assumes the keyColumn'th column of incoming
// rows is a roachpb.Key, and maps it to a stream based on a matching
// span. That is, keys in the nth span will be mapped to the nth stream. The
// keyColumn must be of type DBytes (or optionally DNull if defaultDest
// is set).
type rangeRouter struct {
	routerBase

	alloc rowenc.DatumAlloc
	// b is a temp storage location used during encoding
	b         []byte
	encodings []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding
	spans     []execinfrapb.OutputRouterSpec_RangeRouterSpec_Span
	// defaultDest, if set, sends any row not matching a span to this stream. If
	// not set and a non-matching row is encountered, an error is returned and
	// the router is shut down.
	defaultDest *int
}

var _ execinfra.RowReceiver = &mirrorRouter{}
var _ execinfra.RowReceiver = &hashRouter{}
var _ execinfra.RowReceiver = &rangeRouter{}

func makeMirrorRouter(rb routerBase) (router, error) {
	if len(rb.outputs) < 2 {
		return nil, errors.Errorf("need at least two streams for mirror router")
	}
	return &mirrorRouter{routerBase: rb}, nil
}

// Push is part of the RowReceiver interface.
func (mr *mirrorRouter) Push(
	row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	aggStatus := mr.aggStatus()
	if meta != nil {
		mr.fwdMetadata(meta)
		// fwdMetadata can change the status, re-read it.
		return mr.aggStatus()
	}
	if aggStatus != execinfra.NeedMoreRows {
		return aggStatus
	}

	useSema := mr.shouldUseSemaphore()
	if useSema {
		mr.semaphore <- struct{}{}
	}

	for i := range mr.outputs {
		ro := &mr.outputs[i]
		ro.mu.Lock()
		err := ro.addRowLocked(context.TODO(), row)
		ro.mu.Unlock()
		if err != nil {
			if useSema {
				<-mr.semaphore
			}
			mr.fwdMetadata(&execinfrapb.ProducerMetadata{Err: err})
			atomic.StoreUint32(&mr.aggregatedStatus, uint32(execinfra.ConsumerClosed))
			return execinfra.ConsumerClosed
		}
		ro.mu.cond.Signal()
	}
	if useSema {
		<-mr.semaphore
	}
	return aggStatus
}

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

func makeHashRouter(rb routerBase, hashCols []uint32) (router, error) {
	if len(rb.outputs) < 2 {
		return nil, errors.Errorf("need at least two streams for hash router")
	}
	if len(hashCols) == 0 {
		return nil, errors.Errorf("no hash columns for BY_HASH router")
	}
	return &hashRouter{hashCols: hashCols, routerBase: rb}, nil
}

// Push is part of the RowReceiver interface.
//
// If, according to the hash, the row needs to go to a consumer that's draining
// or closed, the row is silently dropped.
func (hr *hashRouter) Push(
	row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	aggStatus := hr.aggStatus()
	if meta != nil {
		hr.fwdMetadata(meta)
		// fwdMetadata can change the status, re-read it.
		return hr.aggStatus()
	}
	if aggStatus != execinfra.NeedMoreRows {
		return aggStatus
	}

	useSema := hr.shouldUseSemaphore()
	if useSema {
		hr.semaphore <- struct{}{}
	}

	streamIdx, err := hr.computeDestination(row)
	if err == nil {
		ro := &hr.outputs[streamIdx]
		ro.mu.Lock()
		err = ro.addRowLocked(context.TODO(), row)
		ro.mu.Unlock()
		ro.mu.cond.Signal()
	}
	if useSema {
		<-hr.semaphore
	}
	if err != nil {
		hr.fwdMetadata(&execinfrapb.ProducerMetadata{Err: err})
		atomic.StoreUint32(&hr.aggregatedStatus, uint32(execinfra.ConsumerClosed))
		return execinfra.ConsumerClosed
	}
	return aggStatus
}

// computeDestination hashes a row and returns the index of the output stream on
// which it must be sent.
func (hr *hashRouter) computeDestination(row rowenc.EncDatumRow) (int, error) {
	hr.buffer = hr.buffer[:0]
	for _, col := range hr.hashCols {
		if int(col) >= len(row) {
			err := errors.Errorf("hash column %d, row with only %d columns", col, len(row))
			return -1, err
		}
		var err error
		// We choose to not perform the memory accounting for possibly decoded
		// tree.Datum because we will lose the references to row very soon.
		hr.buffer, err = row[col].Fingerprint(context.TODO(), hr.types[col], &hr.alloc, hr.buffer, nil /* acc */)
		if err != nil {
			return -1, err
		}
	}

	// We use CRC32-C because it makes for a decent hash function and is faster
	// than most hashing algorithms (on recent x86 platforms where it is hardware
	// accelerated).
	return int(crc32.Update(0, crc32Table, hr.buffer) % uint32(len(hr.outputs))), nil
}

func makeRangeRouter(
	rb routerBase, spec execinfrapb.OutputRouterSpec_RangeRouterSpec,
) (*rangeRouter, error) {
	if len(spec.Encodings) == 0 {
		return nil, errors.New("missing encodings")
	}
	var defaultDest *int
	if spec.DefaultDest != nil {
		i := int(*spec.DefaultDest)
		defaultDest = &i
	}
	var prevKey []byte
	// Verify spans are sorted and non-overlapping.
	for i, span := range spec.Spans {
		if bytes.Compare(prevKey, span.Start) > 0 {
			return nil, errors.Errorf("span %d not after previous span", i)
		}
		prevKey = span.End
	}
	return &rangeRouter{
		routerBase:  rb,
		spans:       spec.Spans,
		defaultDest: defaultDest,
		encodings:   spec.Encodings,
	}, nil
}

func (rr *rangeRouter) Push(
	row rowenc.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) execinfra.ConsumerStatus {
	aggStatus := rr.aggStatus()
	if meta != nil {
		rr.fwdMetadata(meta)
		// fwdMetadata can change the status, re-read it.
		return rr.aggStatus()
	}

	useSema := rr.shouldUseSemaphore()
	if useSema {
		rr.semaphore <- struct{}{}
	}

	streamIdx, err := rr.computeDestination(row)
	if err == nil {
		ro := &rr.outputs[streamIdx]
		ro.mu.Lock()
		err = ro.addRowLocked(context.TODO(), row)
		ro.mu.Unlock()
		ro.mu.cond.Signal()
	}
	if useSema {
		<-rr.semaphore
	}
	if err != nil {
		rr.fwdMetadata(&execinfrapb.ProducerMetadata{Err: err})
		atomic.StoreUint32(&rr.aggregatedStatus, uint32(execinfra.ConsumerClosed))
		return execinfra.ConsumerClosed
	}
	return aggStatus
}

func (rr *rangeRouter) computeDestination(row rowenc.EncDatumRow) (int, error) {
	var err error
	rr.b = rr.b[:0]
	for _, enc := range rr.encodings {
		col := enc.Column
		rr.b, err = row[col].Encode(rr.types[col], &rr.alloc, enc.Encoding, rr.b)
		if err != nil {
			return 0, err
		}
	}
	i := rr.spanForData(rr.b)
	if i == -1 {
		if rr.defaultDest == nil {
			return 0, errors.New("no span found for key")
		}
		return *rr.defaultDest, nil
	}
	return i, nil
}

// spanForData returns the index of the first span that data is within
// [start, end). A -1 is returned if no such span is found.
func (rr *rangeRouter) spanForData(data []byte) int {
	i := sort.Search(len(rr.spans), func(i int) bool {
		return bytes.Compare(rr.spans[i].End, data) > 0
	})

	// If we didn't find an i where data < end, return an error.
	if i == len(rr.spans) {
		return -1
	}
	// Make sure the Start is <= data.
	if bytes.Compare(rr.spans[i].Start, data) > 0 {
		return -1
	}
	return int(rr.spans[i].Stream)
}
