// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import "github.com/cockroachdb/cockroach/pkg/util/syncutil"

// TODO(dan): Monitor memory usage and spill to disk when necessary.
type changefeedBuffer struct {
	syncutil.Mutex
	buf []changedKVs
	idx int
}

// append adds new kvs to the buffer.
func (b *changefeedBuffer) append(kvs changedKVs) {
	b.Lock()
	if b.idx >= len(b.buf) {
		// Attempt to minimize allocations by reusing the buffer.
		b.buf = b.buf[:0]
		b.idx = 0
	}
	b.buf = append(b.buf, kvs)
	b.Unlock()
}

// get returns the next kvs or false if the buffer is empty.
func (b *changefeedBuffer) get() (changedKVs, bool) {
	var ret changedKVs
	var ok bool
	b.Lock()
	if b.idx < len(b.buf) {
		ret, ok = b.buf[b.idx], true
		b.idx++
	}
	b.Unlock()
	return ret, ok
}
