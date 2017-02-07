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
// Author: Radu Berinde (radu@cockroachlabs.com)
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)
//
// Routers are used by processors to direct outgoing rows to (potentially)
// multiple streams; see docs/RFCS/distributed_sql.md

package distsqlrun

import (
	"hash/crc32"

	"golang.org/x/net/context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func makeRouter(spec *OutputRouterSpec, streams []RowReceiver) (RowReceiver, error) {
	if len(streams) == 0 {
		return nil, errors.Errorf("no streams in router")
	}

	switch spec.Type {
	case OutputRouterSpec_PASS_THROUGH:
		if len(streams) != 1 {
			return nil, errors.Errorf("expected one stream for passthrough router")
		}
		// No router.
		return streams[0], nil

	case OutputRouterSpec_BY_HASH:
		return makeHashRouter(spec.HashColumns, streams)

	case OutputRouterSpec_MIRROR:
		return makeMirrorRouter(streams)

	default:
		return nil, errors.Errorf("router type %s not supported", spec.Type)
	}
}

type routerBase struct {
	streams []RowReceiver
	// The last observed status of the each. This dictates whether we can forward
	// data on each of these channels.
	streamStatus []ConsumerStatus
	// How many of streams are not in the DrainRequested or ConsumerClosed state.
	numNonDrainingStreams int
	// aggregatedStatus maintains a unified view across all streamStatus'es.
	// Namely, if at least one of them is NeedMoreRows, this will be NeedMoreRows.
	// If all of them are ShutdownNoDrain, this will (eventually) be
	// ShutdownNoDrain. Otherwise, this will be DrainRequested.
	aggregatedStatus ConsumerStatus
}

func makeRouterBase(streams []RowReceiver) routerBase {
	return routerBase{
		streams: streams,
		// Initialized to NeedsMoreRows.
		streamStatus:          make([]ConsumerStatus, len(streams)),
		numNonDrainingStreams: len(streams),
	}
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

var _ RowReceiver = &hashRouter{}
var _ RowReceiver = &mirrorRouter{}

var crc32Table = crc32.MakeTable(crc32.Castagnoli)

func makeMirrorRouter(streams []RowReceiver) (*mirrorRouter, error) {
	if len(streams) < 2 {
		return nil, errors.Errorf("need at least two streams for mirror router")
	}
	return &mirrorRouter{
		routerBase: makeRouterBase(streams),
	}, nil
}

func makeHashRouter(hashCols []uint32, streams []RowReceiver) (*hashRouter, error) {
	if len(streams) < 2 {
		return nil, errors.Errorf("need at least two streams for hash router")
	}
	if len(hashCols) == 0 {
		return nil, errors.Errorf("no hash columns for BY_HASH router")
	}
	return &hashRouter{
		routerBase: makeRouterBase(streams),
		hashCols:   hashCols,
	}, nil
}

// ProducerDone is part of the RowReceiver interface.
func (rb *routerBase) ProducerDone() {
	for _, s := range rb.streams {
		s.ProducerDone()
	}
}

// updateStreamState updates the status of one stream and, if this was the last
// open stream, it also updates rb.aggregatedStatus.
func (rb *routerBase) updateStreamState(streamIdx int, newState ConsumerStatus) {
	if newState != rb.streamStatus[streamIdx] && rb.streamStatus[streamIdx] == NeedMoreRows {
		rb.streamStatus[streamIdx] = newState
		// A stream state never goes from draining to non-draining, so we can assume
		// that this stream is now draining or closed.
		rb.numNonDrainingStreams--
	}
	if rb.aggregatedStatus == NeedMoreRows && rb.numNonDrainingStreams == 0 {
		rb.aggregatedStatus = DrainRequested
	}
}

// fwdMetadata forwards a metadata record to the first stream that's still
// accepting data.
func (rb *routerBase) fwdMetadata(meta ProducerMetadata) {
	if meta.Empty() {
		log.Fatalf(context.TODO(), "asked to fwd empty metadata")
	}
	for i := range rb.streams {
		if rb.streamStatus[i] != ConsumerClosed {
			newStatus := rb.streams[i].Push(nil /*row*/, meta)
			rb.updateStreamState(i, newStatus)
			if newStatus != ConsumerClosed {
				// We've successfully forwarded the row.
				return
			}
		}
	}
	// If we got here it means that we couldn't even forward metadata anywhere;
	// all streams are closed.
	rb.aggregatedStatus = ConsumerClosed
}

// Push is part of the RowReceiver interface.
func (mr *mirrorRouter) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	if !meta.Empty() {
		mr.fwdMetadata(meta)
		return mr.aggregatedStatus
	}
	if mr.aggregatedStatus != NeedMoreRows {
		return mr.aggregatedStatus
	}

	// Each row is sent to all the output streams that are still open.
	for i := range mr.streams {
		if mr.streamStatus[i] == NeedMoreRows {
			newStatus := mr.streams[i].Push(row, ProducerMetadata{})
			mr.updateStreamState(i, newStatus)
		}
	}
	return mr.aggregatedStatus
}

// Push is part of the RowReceiver interface.
//
// If, according to the hash, the row needs to go to a consumer that's draining
// or closed, the row is silently dropped.
func (hr *hashRouter) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	if !meta.Empty() {
		hr.fwdMetadata(meta)
		return hr.aggregatedStatus
	}
	if hr.aggregatedStatus != NeedMoreRows {
		return hr.aggregatedStatus
	}

	streamIdx, err := hr.computeDestination(row)
	if err != nil {
		hr.fwdMetadata(ProducerMetadata{Err: err})
		hr.aggregatedStatus = ConsumerClosed
		return ConsumerClosed
	}

	if hr.streamStatus[streamIdx] == NeedMoreRows {
		newStatus := hr.streams[streamIdx].Push(row, ProducerMetadata{})
		hr.updateStreamState(streamIdx, newStatus)
	}
	return hr.aggregatedStatus
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
		hr.buffer, err = row[col].Encode(&hr.alloc, preferredEncoding, hr.buffer)
		if err != nil {
			return -1, err
		}
	}

	// We use CRC32-C because it makes for a decent hash function and is faster
	// than most hashing algorithms (on recent x86 platforms where it is hardware
	// accelerated).
	return int(crc32.Update(0, crc32Table, hr.buffer) % uint32(len(hr.streams))), nil
}
