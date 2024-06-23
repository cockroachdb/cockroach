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
	ft := NewForkTracker(2, 10)

	require.True(t, ft.Append(3, 10, 50))
	require.True(t, ft.Append(4, 25, 65))
	require.True(t, ft.Append(7, 50, 60))
	require.True(t, ft.Append(9, 40, 60))
	require.Equal(t, logMark{term: 2, index: 10}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 60}, ft.write)

	ft.Ack(3, 35)
	require.Equal(t, logMark{term: 3, index: 25}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 60}, ft.write)
	ft.Ack(3, 40)
	require.Equal(t, logMark{term: 3, index: 25}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 60}, ft.write)
	ft.Ack(4, 30)
	require.Equal(t, logMark{term: 4, index: 30}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 60}, ft.write)
	ft.Ack(7, 60)
	require.Equal(t, logMark{term: 7, index: 40}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 60}, ft.write)
	ft.Ack(9, 50)
	require.Equal(t, logMark{term: 9, index: 50}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 60}, ft.write)
	ft.Ack(9, 60)
	require.Equal(t, logMark{term: 9, index: 60}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 60}, ft.write)

	require.True(t, ft.Append(9, 60, 70))
	require.Equal(t, logMark{term: 9, index: 60}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 70}, ft.write)
	ft.Ack(9, 70)
	require.Equal(t, logMark{term: 9, index: 70}, ft.ack)
	require.Equal(t, logMark{term: 9, index: 70}, ft.write)

	require.True(t, ft.Append(10, 10, 100))
	require.Equal(t, logMark{term: 10, index: 10}, ft.ack)
	require.Equal(t, logMark{term: 10, index: 100}, ft.write)
}
