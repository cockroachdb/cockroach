// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TestEntryApplicationStateBuf verifies the entryApplicationStateBuf behavior.
func TestApplicationStateBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var buf cmdAppCtxBuf
	// numStates is chosen arbitrarily.
	const numStates = 5*cmdAppCtxBufNodeSize + 1
	// Test that the len field is properly updated.
	var states []*cmdAppCtx
	for i := 0; i < numStates; i++ {
		assert.Equal(t, i, int(buf.len))
		states = append(states, buf.allocate())
		assert.Equal(t, i+1, int(buf.len))
	}
	// Test that last returns the correct value.
	last := states[len(states)-1]
	assert.Equal(t, last, buf.last())
	// Test the iterator.
	var it cmdAppCtxBufIterator
	i := 0
	for ok := it.init(&buf); ok; ok = it.next() {
		assert.Equal(t, states[i], it.cur())
		i++
	}
	assert.Equal(t, i, numStates) // make sure we saw them all
	assert.True(t, it.isLast())
	// Test clear.
	buf.clear()
	assert.EqualValues(t, buf, cmdAppCtxBuf{})
	assert.Equal(t, 0, int(buf.len))
	assert.Panics(t, func() { buf.last() })
	assert.False(t, it.init(&buf))
	// Test clear on an empty buffer.
	buf.clear()
	assert.EqualValues(t, buf, cmdAppCtxBuf{})
}
