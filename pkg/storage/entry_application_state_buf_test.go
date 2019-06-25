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

// TestApplicationStateBuf is an overly simplistic test of the
// entryApplicationStateBuf behavior.
func TestApplicationStateBuf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var buf entryApplicationStateBuf
	var states []*entryApplicationState
	for i := 0; i < 5*entryApplicationStateBufNodeSize+1; i++ {
		assert.Equal(t, i, int(buf.len))
		states = append(states, buf.allocate())
		assert.Equal(t, i+1, int(buf.len))
	}
	last := states[len(states)-1]
	assert.Equal(t, last, buf.last())
	var it entryApplicationStateBufIterator
	i := 0
	for ok := it.init(&buf); ok; ok = it.next() {
		assert.Equal(t, states[i], it.state())
		i++
	}
	buf.clear()
	assert.Equal(t, 0, int(buf.len))
	assert.Panics(t, func() { buf.last() })
	buf.destroy()
	assert.EqualValues(t, buf, entryApplicationStateBuf{})
}
