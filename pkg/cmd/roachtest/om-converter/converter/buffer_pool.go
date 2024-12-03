// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package converter

import (
	"bytes"
	"sync"
)

// Maximum initial buffer size to prevent excessive memory use
const maxBufferSize = 1 << 20 // 1MB

// bufferPool provides a pool of reusable byte buffers to reduce memory allocations
var bufferPool = sync.Pool{
	New: func() interface{} {
		// Create a new buffer with a modest initial size
		return bytes.NewBuffer(make([]byte, 0, 16*1024)) // 16KB initial capacity
	},
}

// getBuffer retrieves a buffer from the pool or creates a new one
func getBuffer() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

// putBuffer returns a buffer to the pool after resetting it
// and ensuring it doesn't hold excessive memory
func putBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}

	buf.Reset()

	// If the buffer has grown too large, don't return it to the pool
	// Let it be garbage collected instead
	if buf.Cap() > maxBufferSize {
		// Return a new, smaller buffer to the pool instead
		bufferPool.Put(bytes.NewBuffer(make([]byte, 0, 16*1024)))
		return
	}

	bufferPool.Put(buf)
}
