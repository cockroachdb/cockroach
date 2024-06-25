// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForkTracker(t *testing.T) {
	m := func(term, index uint64) LogMark {
		return LogMark{Term: term, Index: index}
	}

	ft := NewForkTracker(m(2, 10))
	check := func(ack, write LogMark, released uint64) {
		require.Equal(t, ack, ft.ack)
		require.Equal(t, write, ft.write)
		require.Equal(t, released, ft.Released())
	}

	require.True(t, ft.Append(m(3, 10), 50))
	require.True(t, ft.Append(m(4, 25), 65))
	require.True(t, ft.Append(m(7, 50), 60))
	require.True(t, ft.Append(m(9, 40), 60))
	check(m(2, 10), m(9, 60), 10)

	require.True(t, ft.Ack(m(3, 35)))
	check(m(3, 35), m(9, 60), 25)
	require.True(t, ft.Ack(m(3, 40)))
	check(m(3, 40), m(9, 60), 25)
	require.True(t, ft.Ack(m(4, 30)))
	check(m(4, 30), m(9, 60), 30)
	require.True(t, ft.Ack(m(7, 60)))
	check(m(7, 60), m(9, 60), 40)
	require.True(t, ft.Ack(m(9, 50)))
	check(m(9, 50), m(9, 60), 50)
	require.True(t, ft.Ack(m(9, 60)))
	check(m(9, 60), m(9, 60), 60)

	require.True(t, ft.Append(m(9, 60), 70))
	check(m(9, 60), m(9, 70), 60)
	require.True(t, ft.Ack(m(9, 70)))
	check(m(9, 70), m(9, 70), 70)

	require.True(t, ft.Append(m(10, 10), 100))
	check(m(10, 10), m(10, 100), 10)
}
