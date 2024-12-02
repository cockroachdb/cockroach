// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowcontainer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

type kvStreamerResultDiskBuffer struct {
	// initialized is set to true when the first Result is Serialize()'d.
	initialized bool
	// container stores all Results that have been Serialize()'d since the last
	// call to Reset().
	// TODO(yuzefovich): at the moment, all serialized results that have been
	// returned to the client still exist in the row container, so they have to
	// be skipped over many times leading to a quadratic behavior. Improve this.
	// One idea is to track the "garbage ratio" and once that exceeds say 50%, a
	// new container is created and non-garbage rows are inserted into it.
	container DiskRowContainer
	// iter, if non-nil, is the iterator currently positioned at iterResultID
	// row. If a new row is added into the container, the iterator becomes
	// invalid, so it'll be closed and nil-ed out.
	iter         RowIterator
	iterResultID int

	engine      diskmap.Factory
	memAcc      mon.BoundAccount
	diskMonitor *mon.BytesMonitor
	rowScratch  rowenc.EncDatumRow
	alloc       tree.DatumAlloc
}

var _ kvstreamer.ResultDiskBuffer = &kvStreamerResultDiskBuffer{}

// NewKVStreamerResultDiskBuffer return a new kvstreamer.ResultDiskBuffer that
// is backed by a disk row container.
func NewKVStreamerResultDiskBuffer(
	engine diskmap.Factory, memAcc mon.BoundAccount, diskMonitor *mon.BytesMonitor,
) kvstreamer.ResultDiskBuffer {
	return &kvStreamerResultDiskBuffer{
		engine:      engine,
		memAcc:      memAcc,
		diskMonitor: diskMonitor,
	}
}

// Serialize implements the kvstreamer.ResultDiskBuffer interface.
func (b *kvStreamerResultDiskBuffer) Serialize(
	ctx context.Context, r *kvstreamer.Result,
) (resultID int, _ error) {
	if !b.initialized {
		var err error
		b.container, err = MakeDiskRowContainer(
			ctx,
			b.memAcc,
			b.diskMonitor,
			inOrderResultsBufferSpillTypeSchema,
			colinfo.ColumnOrdering{},
			b.engine,
		)
		if err != nil {
			return 0, err
		}
		b.initialized = true
		b.rowScratch = make(rowenc.EncDatumRow, len(inOrderResultsBufferSpillTypeSchema))
	}

	if err := serialize(r, b.rowScratch, &b.alloc); err != nil {
		return 0, err
	}
	if err := b.container.AddRow(ctx, b.rowScratch); err != nil {
		return 0, err
	}

	// The iterator became invalid, so we need to close it.
	if b.iter != nil {
		b.iter.Close()
		b.iter = nil
		b.iterResultID = 0
	}

	// The result is spilled as the current last row in the container.
	resultID = b.container.Len() - 1
	return resultID, nil
}

// Deserialize implements the kvstreamer.ResultDiskBuffer interface.
func (b *kvStreamerResultDiskBuffer) Deserialize(
	ctx context.Context, r *kvstreamer.Result, resultID int,
) error {
	// We have to position the iterator at the corresponding rowID first.
	if b.iter == nil {
		b.iter = b.container.NewIterator(ctx)
		b.iter.Rewind()
	}
	if resultID < b.iterResultID {
		b.iter.Rewind()
		b.iterResultID = 0
	}
	for b.iterResultID < resultID {
		b.iter.Next()
		b.iterResultID++
	}
	// Now we take the row representing the Result and deserialize it into r.
	serialized, err := b.iter.EncRow()
	if err != nil {
		return err
	}
	return deserialize(r, serialized, &b.alloc)
}

// Reset implements the kvstreamer.ResultDiskBuffer interface.
func (b *kvStreamerResultDiskBuffer) Reset(ctx context.Context) error {
	if !b.initialized {
		return nil
	}
	if b.iter != nil {
		b.iter.Close()
		b.iter = nil
		b.iterResultID = 0
	}
	return b.container.UnsafeReset(ctx)
}

// Close implements the kvstreamer.ResultDiskBuffer interface.
func (b *kvStreamerResultDiskBuffer) Close(ctx context.Context) {
	if b.initialized {
		if b.iter != nil {
			b.iter.Close()
			b.iter = nil
		}
		b.container.Close(ctx)
	}
}

// inOrderResultsBufferSpillTypeSchema is the type schema of a single
// kvstreamer.Result that is spilled to disk.
//
// It contains all the information except for 'Position', 'memoryTok',
// 'subRequestIdx', 'subRequestDone', and 'scanComplete' fields which are kept
// in-memory (because they are allocated in
// kvstreamer.inOrderBufferedResult.Result anyway).
var inOrderResultsBufferSpillTypeSchema = []*types.T{
	types.Bool, // isGet
	// GetResp.Value:
	//	RawBytes []byte
	//	Timestamp hlc.Timestamp
	//	  WallTime int64
	//	  Logical int32
	types.Bytes, types.Int, types.Int,
	// ScanResp:
	//  BatchResponses [][]byte
	types.BytesArray,
}

type resultSerializationIndex int

const (
	isGetIdx resultSerializationIndex = iota
	getRawBytesIdx
	getTSWallTimeIdx
	getTSLogicalIdx
	scanBatchResponsesIdx
)

// serialize writes the serialized representation of the kvstreamer.Result into
// row according to inOrderResultsBufferSpillTypeSchema.
func serialize(r *kvstreamer.Result, row rowenc.EncDatumRow, alloc *tree.DatumAlloc) error {
	row[isGetIdx] = rowenc.EncDatum{Datum: tree.MakeDBool(r.GetResp != nil)}
	if r.GetResp != nil && r.GetResp.Value != nil {
		// We have a non-empty Get response.
		v := r.GetResp.Value
		row[getRawBytesIdx] = rowenc.EncDatum{Datum: alloc.NewDBytes(tree.DBytes(v.RawBytes))}
		row[getTSWallTimeIdx] = rowenc.EncDatum{Datum: alloc.NewDInt(tree.DInt(v.Timestamp.WallTime))}
		row[getTSLogicalIdx] = rowenc.EncDatum{Datum: alloc.NewDInt(tree.DInt(v.Timestamp.Logical))}
		row[scanBatchResponsesIdx] = rowenc.EncDatum{Datum: tree.DNull}
	} else {
		row[getRawBytesIdx] = rowenc.EncDatum{Datum: tree.DNull}
		row[getTSWallTimeIdx] = rowenc.EncDatum{Datum: tree.DNull}
		row[getTSLogicalIdx] = rowenc.EncDatum{Datum: tree.DNull}
		if r.GetResp != nil {
			// We have an empty Get response.
			row[scanBatchResponsesIdx] = rowenc.EncDatum{Datum: tree.DNull}
		} else {
			// We have a Scan response.
			batchResponses := tree.NewDArray(types.Bytes)
			batchResponses.Array = make(tree.Datums, 0, len(r.ScanResp.BatchResponses))
			for _, b := range r.ScanResp.BatchResponses {
				if err := batchResponses.Append(alloc.NewDBytes(tree.DBytes(b))); err != nil {
					return err
				}
			}
			row[scanBatchResponsesIdx] = rowenc.EncDatum{Datum: batchResponses}
		}
	}
	return nil
}

// deserialize updates r in-place based on row which contains the serialized
// state of the kvstreamer.Result according to
// inOrderResultsBufferSpillTypeSchema.
//
// 'Position', 'memoryTok', 'subRequestIdx', 'subRequestDone', and
// 'scanComplete' fields are left unchanged since those aren't serialized.
func deserialize(r *kvstreamer.Result, row rowenc.EncDatumRow, alloc *tree.DatumAlloc) error {
	for i := range row {
		if err := row[i].EnsureDecoded(inOrderResultsBufferSpillTypeSchema[i], alloc); err != nil {
			return err
		}
	}
	if isGet := tree.MustBeDBool(row[isGetIdx].Datum); isGet {
		r.GetResp = &kvpb.GetResponse{}
		if row[getRawBytesIdx].Datum != tree.DNull {
			r.GetResp.Value = &roachpb.Value{
				RawBytes: []byte(tree.MustBeDBytes(row[getRawBytesIdx].Datum)),
				Timestamp: hlc.Timestamp{
					WallTime: int64(tree.MustBeDInt(row[getTSWallTimeIdx].Datum)),
					Logical:  int32(tree.MustBeDInt(row[getTSLogicalIdx].Datum)),
				},
			}
		}
	} else {
		r.ScanResp = &kvpb.ScanResponse{}
		batchResponses := tree.MustBeDArray(row[scanBatchResponsesIdx].Datum)
		r.ScanResp.BatchResponses = make([][]byte, batchResponses.Len())
		for i := range batchResponses.Array {
			r.ScanResp.BatchResponses[i] = []byte(tree.MustBeDBytes(batchResponses.Array[i]))
		}
	}
	return nil
}
