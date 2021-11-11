// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

type writerPoolImpl struct {
	mu struct {
		syncutil.Mutex
		activeWriters map[Writer]struct{}
	}

	new func() Writer
}

var _ writerPool = &writerPoolImpl{}

func newWriterPoolImpl(new func() Writer) *writerPoolImpl {
	w := &writerPoolImpl{
		new: new,
	}
	w.mu.activeWriters = make(map[Writer]struct{})
	return w
}

// GetWriter implements the writerPool interface.
// TODO(azhng): current writerPollImpl is very primitive, it returns a
//  new instance of Writer each time GetWriter() is called. This means that
//  each connExecutor will have its own copy of Writer. The underlying
//  implementation of Writer is ConcurrentWriteBuffer, which is capable of
//  efficiently handle concurrent writes. We can improve Writer allocation
//  by making multiple connExecutors share a single Writer.
//  One possible idea is to pre-allocate a pool of Writers and store them
//  in a min-heap sorted by their ref-count. Each time GetWriter() is called,
//  we find a Writer from the min-heap with least ref-count and give it back to
//  the connExecutor, and increment its ref-count. Once the connExecutor closes,
//  the ref-count is decremented.
func (w *writerPoolImpl) GetWriter() Writer {
	writeBuffer := w.new()
	w.mu.Lock()
	w.mu.activeWriters[writeBuffer] = struct{}{}
	w.mu.Unlock()
	return writeBuffer
}

// disconnect implements the writerPool interface.
func (w *writerPoolImpl) disconnect(writer Writer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.mu.activeWriters, writer)
}
