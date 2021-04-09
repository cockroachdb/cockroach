// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftentry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRingBuffer_Iterator(t *testing.T) {
	b := ringBuf{}
	b.add(newEntries(10, 20, 1))
	require.EqualValues(t, 10, b.len)
	b.truncateFrom(1)
	require.EqualValues(t, 0, b.len)
	it := first(&b)
	require.False(t, it.valid(&b))
}
