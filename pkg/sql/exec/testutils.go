// Copyright 2019 The Cockroach Authors.
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

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
)

// BatchBuffer exposes a buffer of coldata.Batches through an Operator
// interface. If there are no batches to return, Next will panic.
type BatchBuffer struct {
	buffer []coldata.Batch
}

var _ Operator = &BatchBuffer{}

// NewBatchBuffer creates a new BatchBuffer.
func NewBatchBuffer() *BatchBuffer {
	return &BatchBuffer{
		buffer: make([]coldata.Batch, 0, 2),
	}
}

// Add adds a batch to the buffer.
func (b *BatchBuffer) Add(batch coldata.Batch) {
	b.buffer = append(b.buffer, batch)
}

// Init is part of the Operator interface.
func (b *BatchBuffer) Init() {}

// Next is part of the Operator interface.
func (b *BatchBuffer) Next(context.Context) coldata.Batch {
	batch := b.buffer[0]
	b.buffer = b.buffer[1:]
	return batch
}
