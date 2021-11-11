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

	New func() Writer
}

var _ writerPool = &writerPoolImpl{}

func newWriterPoolImpl(new func() Writer) *writerPoolImpl {
	w := &writerPoolImpl{
		New: new,
	}
	w.mu.activeWriters = make(map[Writer]struct{})
	return w
}

// GetWriter implements the writerPool interface.
func (w *writerPoolImpl) GetWriter() Writer {
	writeBuffer := w.New()
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
