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
//
// Routers are used by processors to direct outgoing rows to (potentially)
// multiple streams; see docs/RFCS/distributed_sql.md

package distsql

import (
	"hash/crc32"

	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/pkg/errors"
)

func makeRouter(spec *OutputRouterSpec, streams []RowReceiver) (RowReceiver, error) {
	switch len(streams) {
	case 0:
		return nil, errors.Errorf("no streams in router")
	case 1:
		// Special passthrough case - no router.
		return streams[0], nil
	}

	switch spec.Type {
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
	err     error
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
	return &mirrorRouter{
		routerBase: routerBase{streams: streams},
	}, nil
}

func makeHashRouter(hashCols []uint32, streams []RowReceiver) (*hashRouter, error) {
	if len(hashCols) == 0 {
		return nil, errors.Errorf("no hash columns for BY_HASH router")
	}
	return &hashRouter{
		routerBase: routerBase{streams: streams},
		hashCols:   hashCols,
	}, nil
}

// Close is part of the RowReceiver interface.
func (rb *routerBase) Close(err error) {
	if rb.err != nil {
		// Any error we ran into takes precedence.
		err = rb.err
	}
	for _, s := range rb.streams {
		s.Close(err)
	}
}

// PushRow is part of the RowReceiver interface.
func (mr *mirrorRouter) PushRow(row sqlbase.EncDatumRow) bool {
	if mr.err != nil {
		return false
	}

	// Each row is sent to all the output streams, returning false here if a
	// stream in particular does not need more rows or if none of them do seems
	// unnecessary.
	for _, s := range mr.streams {
		s.PushRow(row)
	}
	return true
}

// PushRow is part of the RowReceiver interface.
func (hr *hashRouter) PushRow(row sqlbase.EncDatumRow) bool {
	if hr.err != nil {
		return false
	}
	hr.buffer = hr.buffer[:0]
	for _, col := range hr.hashCols {
		if int(col) >= len(row) {
			hr.err = errors.Errorf("hash column %d, stream with only %d columns", col, len(row))
			return false
		}
		// TODO(radu): we should choose an encoding that is already available as
		// much as possible. However, we cannot decide this locally as multiple
		// nodes may be doing the same hashing and the encodings need to match. The
		// encoding needs to be determined at planning time.
		hr.buffer, hr.err = row[col].Encode(&hr.alloc, preferredEncoding, hr.buffer)
		if hr.err != nil {
			return false
		}
	}

	// We use CRC32-C because it makes for a decent hash function and is faster
	// than most hashing algorithms (on recent x86 platforms where it is hardware
	// accelerated).
	streamIdx := crc32.Update(0, crc32Table, hr.buffer) % uint32(len(hr.streams))

	// We can't return false if this stream happened to not need any more rows. We
	// could only return false once all streams returned false, but that seems of
	// limited benefit.
	_ = hr.streams[streamIdx].PushRow(row)
	return true
}
