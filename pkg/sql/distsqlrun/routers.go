// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Routers are used by processors to direct outgoing rows to (potentially)
// multiple streams; see docs/RFCS/distributed_sql.md

package distsqlrun

import (
	"bytes"
	"context"
	"hash/crc32"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type router interface {
	RowReceiver
	startable
	init(flowCtx *FlowCtx, types []sqlbase.ColumnType)
}

// makeRouter creates a router. The router's init must be called before the
// router can be started.
//
// Pass-through routers are not supported; the higher layer is expected to elide
// them.
func makeRouter(spec *OutputRouterSpec, streams []RowReceiver) (router, error) {
	if len(streams) == 0 {
		return nil, errors.Errorf("no streams in router")
	}

	var rb routerBase
	rb.setupStreams(streams, spec.DisableBuffering)

	switch spec.Type {
	case OutputRouterSpec_BY_HASH:
		return makeHashRouter(rb, spec.HashColumns)

	case OutputRouterSpec_MIRROR:
		return makeMirrorRouter(rb)

	case OutputRouterSpec_BY_RANGE:
		return makeRangeRouter(rb, spec.RangeRouterSpec)

	default:
		return nil, errors.Errorf("router type %s not supported", spec.Type)
	}
}

const routerRowBufSize = rowChannelBufSize

// routerOutput is the data associated with one router consumer.
type routerOutput struct {
	stream RowReceiver
	mu     struct {
		syncutil.Mutex
		// cond is signaled whenever the main router routine adds a metadata item, a
		// row, or sets producerDone.
		cond         *sync.Cond
		streamStatus ConsumerStatus

		metadataBuf []*ProducerMetadata
		// The "level 1" row buffer is used first, to avoid going through the row
		// container if we don't need to buffer many rows. The buffer is a circular
		// FIFO queue, with rowBufLen elements and the left-most (oldest) element at
		// rowBufLeft.
		rowBuf                [routerRowBufSize]sqlbase.EncDatumRow
		rowBufLeft, rowBufLen uint32

		// The "level 2" rowContainer is used when we need to buffer more rows than
		// rowBuf allows. The row container always contains rows "older" than those
		// in rowBuf. The oldest rows are at the beginning of the row container.
		// TODO(radu,arjun): fall back to a disk container when it gets too big.
		rowContainer memRowContainer
		producerDone bool
	}
	// TODO(radu): add padding of size sys.CacheLineSize to ensure there is no
	// false-sharing?
}

func (ro *routerOutput) addMetadataLocked(meta *ProducerMetadata) {
	// We don't need any fancy buffering because normally there is not a lot of
	// metadata being passed around.
	ro.mu.metadataBuf = append(ro.mu.metadataBuf, meta)
}

// addRowLocked adds a row to rowBuf (potentially evicting the oldest row into
// rowContainer).
func (ro *routerOutput) addRowLocked(ctx context.Context, row sqlbase.EncDatumRow) error {
	if ro.mu.streamStatus != NeedMoreRows {
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

func (ro *routerOutput) popRowsLocked(rowBuf []sqlbase.EncDatumRow) []sqlbase.EncDatumRow {
	n := 0
	// First try to get rows from the row container.
	for ; n < len(rowBuf) && ro.mu.rowContainer.Len() > 0; n++ {
		// TODO(radu): use an EncDatumRowAlloc?
		row := ro.mu.rowContainer.EncRow(0)
		rowBuf[n] = make(sqlbase.EncDatumRow, len(row))
		copy(rowBuf[n], row)
		ro.mu.rowContainer.PopFirst()
	}
	// If the row container is empty, get more rows from the row buffer.
	for ; n < len(rowBuf) && ro.mu.rowBufLen > 0; n++ {
		rowBuf[n] = ro.mu.rowBuf[ro.mu.rowBufLeft]
		ro.mu.rowBufLeft = (ro.mu.rowBufLeft + 1) % routerRowBufSize
		ro.mu.rowBufLen--
	}
	return rowBuf[:n]
}

// See the comment for routerBase.semaphoreCount.
const semaphorePeriod = 8

type routerBase struct {
	types []sqlbase.ColumnType

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
}

func (rb *routerBase) aggStatus() ConsumerStatus {
	return ConsumerStatus(atomic.LoadUint32(&rb.aggregatedStatus))
}

func (rb *routerBase) setupStreams(streams []RowReceiver, disableBuffering bool) {
	rb.numNonDrainingStreams = int32(len(streams))
	n := len(streams)
	if disableBuffering {
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
		ro.mu.cond = sync.NewCond(&ro.mu.Mutex)
		ro.mu.streamStatus = NeedMoreRows
	}
}

// init must be called after setupStreams but before start.
func (rb *routerBase) init(flowCtx *FlowCtx, types []sqlbase.ColumnType) {
	rb.types = types
	for i := range rb.outputs {
		// This method must be called before we start() so we don't need
		// to take the mutex.
		rb.outputs[i].mu.rowContainer.init(nil /* ordering */, types, flowCtx.NewEvalCtx())
		// Initialize any outboxes.
		if o, ok := rb.outputs[i].stream.(*outbox); ok {
			o.init(types)
		}
	}
}

// start must be called after init.
func (rb *routerBase) start(ctx context.Context, wg *sync.WaitGroup, ctxCancel context.CancelFunc) {
	wg.Add(len(rb.outputs))
	for i := range rb.outputs {
		go func(rb *routerBase, ro *routerOutput, wg *sync.WaitGroup) {
			rowBuf := make([]sqlbase.EncDatumRow, routerRowBufSize)
			streamStatus := NeedMoreRows
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

				// Send any rows that have been buffered. We grab multiple rows at a
				// time to reduce contention.
				if rows := ro.popRowsLocked(rowBuf); len(rows) > 0 {
					ro.mu.Unlock()
					rb.semaphore <- struct{}{}
					for _, row := range rows {
						status := ro.stream.Push(row, nil)
						rb.updateStreamState(&streamStatus, status)
					}
					<-rb.semaphore
					ro.mu.Lock()
					ro.mu.streamStatus = streamStatus
					continue
				}

				// No rows or metadata buffered; see if the producer is done.
				if ro.mu.producerDone {
					ro.stream.ProducerDone()
					break
				}

				// Nothing to do; wait.
				ro.mu.cond.Wait()
			}
			ro.mu.rowContainer.Close(ctx)
			ro.mu.Unlock()
			wg.Done()
		}(rb, &rb.outputs[i], wg)
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
func (rb *routerBase) updateStreamState(streamStatus *ConsumerStatus, newState ConsumerStatus) {
	if newState != *streamStatus {
		if *streamStatus == NeedMoreRows {
			// A stream state never goes from draining to non-draining, so we can assume
			// that this stream is now draining or closed.
			if atomic.AddInt32(&rb.numNonDrainingStreams, -1) == 0 {
				// Update aggregatedStatus, if the current value is NeedMoreRows.
				atomic.CompareAndSwapUint32(
					&rb.aggregatedStatus,
					uint32(NeedMoreRows),
					uint32(DrainRequested),
				)
			}
		}
		*streamStatus = newState
	}
}

// fwdMetadata forwards a metadata record to the first stream that's still
// accepting data.
func (rb *routerBase) fwdMetadata(meta *ProducerMetadata) {
	if meta == nil {
		log.Fatalf(context.TODO(), "asked to fwd empty metadata")
	}

	rb.semaphore <- struct{}{}

	for i := range rb.outputs {
		ro := &rb.outputs[i]
		ro.mu.Lock()
		if ro.mu.streamStatus != ConsumerClosed {
			ro.addMetadataLocked(meta)
			ro.mu.Unlock()
			ro.mu.cond.Signal()
			<-rb.semaphore
			return
		}
		ro.mu.Unlock()
	}

	<-rb.semaphore
	// If we got here it means that we couldn't even forward metadata anywhere;
	// all streams are closed.
	atomic.StoreUint32(&rb.aggregatedStatus, uint32(ConsumerClosed))
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
	alloc    sqlbase.DatumAlloc
}

// rangeRouter is a router that assumes the keyColumn'th column of incoming
// rows is a roachpb.Key, and maps it to a stream based on a matching
// span. That is, keys in the nth span will be mapped to the nth stream. The
// keyColumn must be of type DBytes (or optionally DNull if defaultDest
// is set).
type rangeRouter struct {
	routerBase

	alloc sqlbase.DatumAlloc
	// b is a temp storage location used during encoding
	b         []byte
	encodings []OutputRouterSpec_RangeRouterSpec_ColumnEncoding
	spans     []OutputRouterSpec_RangeRouterSpec_Span
	// defaultDest, if set, sends any row not matching a span to this stream. If
	// not set and a non-matching row is encountered, an error is returned and
	// the router is shut down.
	defaultDest *int
}

var _ RowReceiver = &mirrorRouter{}
var _ RowReceiver = &hashRouter{}
var _ RowReceiver = &rangeRouter{}

func makeMirrorRouter(rb routerBase) (router, error) {
	if len(rb.outputs) < 2 {
		return nil, errors.Errorf("need at least two streams for mirror router")
	}
	return &mirrorRouter{routerBase: rb}, nil
}

// Push is part of the RowReceiver interface.
func (mr *mirrorRouter) Push(row sqlbase.EncDatumRow, meta *ProducerMetadata) ConsumerStatus {
	aggStatus := mr.aggStatus()
	if meta != nil {
		mr.fwdMetadata(meta)
		return aggStatus
	}
	if aggStatus != NeedMoreRows {
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
			mr.fwdMetadata(&ProducerMetadata{Err: err})
			atomic.StoreUint32(&mr.aggregatedStatus, uint32(ConsumerClosed))
			return ConsumerClosed
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
func (hr *hashRouter) Push(row sqlbase.EncDatumRow, meta *ProducerMetadata) ConsumerStatus {
	aggStatus := hr.aggStatus()
	if meta != nil {
		hr.fwdMetadata(meta)
		// fwdMetadata can change the status, re-read it.
		return hr.aggStatus()
	}
	if aggStatus != NeedMoreRows {
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
		hr.fwdMetadata(&ProducerMetadata{Err: err})
		atomic.StoreUint32(&hr.aggregatedStatus, uint32(ConsumerClosed))
		return ConsumerClosed
	}
	return aggStatus
}

// computeDestination hashes a row and returns the index of the output stream on
// which it must be sent.
func (hr *hashRouter) computeDestination(row sqlbase.EncDatumRow) (int, error) {
	hr.buffer = hr.buffer[:0]
	for _, col := range hr.hashCols {
		if int(col) >= len(row) {
			err := errors.Errorf("hash column %d, row with only %d columns", col, len(row))
			return -1, err
		}
		// TODO(radu): we should choose an encoding that is already available as
		// much as possible. However, we cannot decide this locally as multiple
		// nodes may be doing the same hashing and the encodings need to match. The
		// encoding needs to be determined at planning time. #13829
		var err error
		hr.buffer, err = row[col].Encode(&hr.types[col], &hr.alloc, preferredEncoding, hr.buffer)
		if err != nil {
			return -1, err
		}
	}

	// We use CRC32-C because it makes for a decent hash function and is faster
	// than most hashing algorithms (on recent x86 platforms where it is hardware
	// accelerated).
	return int(crc32.Update(0, crc32Table, hr.buffer) % uint32(len(hr.outputs))), nil
}

func makeRangeRouter(rb routerBase, spec OutputRouterSpec_RangeRouterSpec) (*rangeRouter, error) {
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

func (rr *rangeRouter) Push(row sqlbase.EncDatumRow, meta *ProducerMetadata) ConsumerStatus {
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
		rr.fwdMetadata(&ProducerMetadata{Err: err})
		atomic.StoreUint32(&rr.aggregatedStatus, uint32(ConsumerClosed))
		return ConsumerClosed
	}
	return aggStatus
}

func (rr *rangeRouter) computeDestination(row sqlbase.EncDatumRow) (int, error) {
	var err error
	rr.b = rr.b[:0]
	for _, enc := range rr.encodings {
		col := enc.Column
		rr.b, err = row[col].Encode(&rr.types[col], &rr.alloc, enc.Encoding, rr.b)
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
	// Make sure the start is <= data.
	if bytes.Compare(rr.spans[i].Start, data) > 0 {
		return -1
	}
	return int(rr.spans[i].Stream)
}
