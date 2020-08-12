// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

// TestReplicatedCmdBuf verifies the replicatedCmdBuf behavior.
func TestReplicatedCmdBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var buf replicatedCmdBuf
	// numStates is chosen arbitrarily.
	const numStates = 5*replicatedCmdBufNodeSize + 1
	// Test that the len field is properly updated.
	var states []*replicatedCmd
	for i := 0; i < numStates; i++ {
		assert.Equal(t, i, int(buf.len))
		states = append(states, buf.allocate())
		assert.Equal(t, i+1, int(buf.len))
	}
	// Test the iterator.
	var it replicatedCmdBufSlice
	i := 0
	for it.init(&buf); it.Valid(); it.Next() {
		assert.Equal(t, states[i], it.cur())
		i++
	}
	assert.Equal(t, i, numStates) // make sure we saw them all
	// Test clear.
	buf.clear()
	assert.EqualValues(t, buf, replicatedCmdBuf{})
	assert.Equal(t, 0, int(buf.len))
	it.init(&buf)
	assert.False(t, it.Valid())
	// Test clear on an empty buffer.
	buf.clear()
	assert.EqualValues(t, buf, replicatedCmdBuf{})
}
