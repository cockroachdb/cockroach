// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colcontainer

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
)

// PartitionedQueue is the abstraction for on-disk storage.
type PartitionedQueue interface {
	// Enqueue adds the batch to the end of the partitionIdx'th partition. If a
	// partition at that index does not exist, a new one is created.
	Enqueue(partitionIdx int, batch coldata.Batch) error
	// Dequeue removes and returns the batch from the front of the
	// partitionIdx'th partition. If the partition is empty, or no partition at
	// that index was Enqueued to, a zero-length batch is returned.
	Dequeue(partitionIdx int, batch coldata.Batch) error
	// Close closes all partitions created.
	Close() error
}

type PartitionedDiskQueue struct {
	typs []coltypes.T
	cfg  DiskQueueCfg

	partitionIdxToIndex map[int]int
	partitions          []Queue
}

// NewPartitionedDiskQueue creates a PartitionedDiskQueue whose partitions are
// all on-disk queues. Note that diskQueues will be lazily created when
// enqueueing to a new partition. Each new partition will use
// cfg.BufferSizeBytes, so memory usage may increase in an unbounded fashion.
func NewPartitionedDiskQueue(typs []coltypes.T, cfg DiskQueueCfg) *PartitionedDiskQueue {
	return &PartitionedDiskQueue{
		typs:                typs,
		cfg:                 cfg,
		partitionIdxToIndex: make(map[int]int),
		partitions:          make([]Queue, 0),
	}
}

func (p *PartitionedDiskQueue) Enqueue(partitionIdx int, batch coldata.Batch) error {
	idx, ok := p.partitionIdxToIndex[partitionIdx]
	if !ok {
		// Partition has not been created yet.
		q, err := NewDiskQueue(p.typs, p.cfg)
		if err != nil {
			return err
		}
		idx = len(p.partitions)
		p.partitions = append(p.partitions, q)
		p.partitionIdxToIndex[partitionIdx] = idx
	}
	return p.partitions[idx].Enqueue(batch)
}

func (p *PartitionedDiskQueue) Dequeue(partitionIdx int, batch coldata.Batch) error {
	idx, ok := p.partitionIdxToIndex[partitionIdx]
	if !ok {
		batch.SetLength(0)
		return nil
	}
	notEmpty, err := p.partitions[idx].Dequeue(batch)
	if err != nil {
		return err
	}
	if !notEmpty {
		// The queue was empty.
		batch.SetLength(0)
	}
	return nil
}

// Close closes all the PartitionedDiskQueue's partitions. If an error is
// encountered, the PartitionedDiskQueue will attempt to close all partitions
// anyway and return the last error encountered.
func (p *PartitionedDiskQueue) Close() error {
	var lastErr error
	for _, q := range p.partitions {
		lastErr = q.Close()
	}
	return lastErr
}
