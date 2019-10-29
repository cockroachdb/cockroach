// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package colserde

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

type PebbleQueue struct {
	cfg  PebbleQueueCfg
	done bool

	serializedBuf bytes.Buffer
	// serializer is serializes Enqueued batches into bytes. A nil serializer indicates that no more batches will be
	// enqueued.
	serializer        *FileSerializer
	deserializerState struct {
		*FileDeserializer
		curBatch int
	}

	diskMap  diskmap.SortedDiskMap
	e        diskmap.Factory
	iterator diskmap.SortedDiskMapIterator
	writer   diskmap.SortedDiskMapBatchWriter

	batchToReturn coldata.Batch
	// currentWriteKey is the key that should be created to store batches.
	currentWriteKey int
	scratchKey      []byte
	// currentReadKey is the key of the next batch to read.
	currentReadKey int
}

func NewPebbleQueue(cfg PebbleQueueCfg) (*PebbleQueue, error) {
	e, err := engine.NewPebbleTempEngine(base.TempStorageConfig{Path: cfg.Path}, base.StoreSpec{})
	if err != nil {
		return nil, err
	}
	diskMap := e.NewSortedDiskMap()
	q := &PebbleQueue{
		cfg:           cfg,
		diskMap:       diskMap,
		e:             e,
		writer:        diskMap.NewBatchWriterCapacity(cfg.BufferSizeBytes),
		batchToReturn: coldata.NewMemBatch(queueTyps),
		scratchKey:    make([]byte, 4),
	}
	s, err := NewFileSerializer(&q.serializedBuf, queueTyps)
	if err != nil {
		return nil, err
	}
	q.serializer = s
	return q, nil
}

func (q *PebbleQueue) flushSerializer() error {
	if q.serializer == nil {
		return nil
	}
	if q.serializer.Written() == 0 {
		return nil
	}
	if err := q.serializer.Finish(); err != nil {
		return err
	}
	q.scratchKey = q.scratchKey[:0]
	q.scratchKey = encoding.EncodeVarintAscending(q.scratchKey, int64(q.currentWriteKey))
	if err := q.writer.Put(q.scratchKey, q.serializedBuf.Bytes()); err != nil {
		return err
	}

	// The problem here seems that we could be overwriting `serializedBuf` before *actually* flushing to disk.
	q.currentWriteKey++
	// It's ok to reset the buffer here, since the bytes were copied by writer.Put.
	q.serializedBuf.Reset()
	return q.serializer.Reset(&q.serializedBuf)
}

func (q *PebbleQueue) Enqueue(b coldata.Batch) error {
	if b.Length() == 0 {
		if err := q.flushSerializer(); err != nil {
			return err
		}
		q.serializer = nil
		q.done = true
		return nil
	}
	if err := q.serializer.AppendBatch(b); err != nil {
		return err
	}
	if q.serializer.Written() >= q.cfg.MaxValueSizeBytes {
		return q.flushSerializer()
	}
	return nil
}

func (q *PebbleQueue) Dequeue() (coldata.Batch, error) {
	if q.serializer != nil && q.serializer.Written() > 0 {
		if err := q.flushSerializer(); err != nil {
			return nil, err
		}
	}

	if err := q.writer.Flush(); err != nil {
		return nil, err
	}

	if q.deserializerState.FileDeserializer != nil {
		numBatches := q.deserializerState.NumBatches()
		if numBatches == 0 {
			q.done = true
			q.batchToReturn.SetLength(0)
			return q.batchToReturn, nil
		}

		// Get next batch and return it while we have in-memory stuff.
		if q.deserializerState.curBatch < numBatches {
			if err := q.deserializerState.GetBatch(q.deserializerState.curBatch, q.batchToReturn); err != nil {
				return nil, err
			}
			q.deserializerState.curBatch++
			return q.batchToReturn, nil
		}

		// Deserializer exhausted.
		if err := q.deserializerState.Close(); err != nil {
			return nil, err
		}
		q.deserializerState.FileDeserializer = nil
	}

	if q.currentReadKey == q.currentWriteKey {
		// This next value to read has not been written yet.
		if q.done {
			q.batchToReturn.SetLength(0)
			return q.batchToReturn, nil
		}
		return nil, nil
	}

	if q.iterator == nil || !q.cfg.SkipRecreateIter {
		// Note that q.deserializer must be nil at this point due to the previous if statement.
		if q.iterator != nil {
			q.iterator.Close()
		}
		q.iterator = q.diskMap.NewIterator()
		q.scratchKey = q.scratchKey[:0]
		q.scratchKey = encoding.EncodeVarintAscending(q.scratchKey, int64(q.currentReadKey))
		q.iterator.SeekGE(q.scratchKey)
	} else {
		q.iterator.Next()
	}

	if ok, err := q.iterator.Valid(); !ok || err != nil {
		if err == nil {
			err = errors.New("invalid key")
		}
		return nil, err
	}

	q.currentReadKey++

	deserializer, err := NewFileDeserializerFromBytes(q.iterator.UnsafeValue())
	if err != nil {
		return nil, err
	}
	q.deserializerState.FileDeserializer = deserializer
	q.deserializerState.curBatch = 0

	return q.Dequeue()
}

func (q *PebbleQueue) Close() error {
	ctx := context.Background()
	if q.serializer != nil {
		if err := q.flushSerializer(); err != nil {
			return err
		}
		q.serializer = nil
	}
	if q.writer != nil {
		if err := q.writer.Close(ctx); err != nil {
			return err
		}
	}
	if q.iterator != nil {
		q.iterator.Close()
	}
	q.diskMap.Close(ctx)
	q.e.Close()
	return nil
}
