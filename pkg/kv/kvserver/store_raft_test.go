// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvserver

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRaftReceiveQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	g := metric.NewGauge(metric.Metadata{})
	m := mon.NewUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource, g,
		nil, math.MaxInt64, st,
	)
	qs := raftReceiveQueues{mon: m}

	const r1 = roachpb.RangeID(1)
	const r5 = roachpb.RangeID(5)

	qs.Load(r1)
	qs.Load(r5)
	require.Zero(t, m.AllocBytes())

	q1, loaded := qs.LoadOrCreate(r1, 10 /* maxLen */)
	require.Zero(t, m.AllocBytes())
	require.False(t, loaded)
	{
		q1x, loadedx := qs.LoadOrCreate(r1, 10 /* maxLen */)
		require.True(t, loadedx)
		require.Equal(t, q1, q1x)
	}
	require.Zero(t, m.AllocBytes())

	e1 := &kvserverpb.RaftMessageRequest{Message: raftpb.Message{Entries: []raftpb.Entry{
		{Data: []byte("foo bar baz")}}}}
	e5 := &kvserverpb.RaftMessageRequest{Message: raftpb.Message{Entries: []raftpb.Entry{
		{Data: []byte("xxxxlxlxlxlxllxlxlxlxlxxlxllxlxlxlxlxl")}}}}
	n1 := int64(e1.Size())
	n5 := int64(e5.Size())

	// Append an entry.
	{
		shouldQ, size, appended := q1.Append(e1, nil /* stream */)
		require.True(t, appended)
		require.True(t, shouldQ)
		require.Equal(t, n1, size)
		require.Equal(t, n1, q1.acc.Used())
		// NB: the monitor allocates in chunks so it will have allocated more than n1.
		// We don't check these going forward, as we've now verified that they're hooked up.
		require.GreaterOrEqual(t, m.AllocBytes(), n1)
		require.Equal(t, m.AllocBytes(), g.Value())
	}

	{
		sl, ok := q1.Drain()
		require.True(t, ok)
		require.Len(t, sl, 1)
		require.Equal(t, e1, sl[0].req)
		require.Zero(t, q1.acc.Used())
	}

	// Append a first element (again).
	{
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.True(t, shouldQ)
		require.True(t, appended)
		require.Equal(t, n1, q1.acc.Used())
	}

	// Add a second element.
	{
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.False(t, shouldQ) // not first entry in queue
		require.True(t, appended)
		require.Equal(t, 2*n1, q1.acc.Used())
	}

	// Now interleave creation of a second queue.
	q5, loaded := qs.LoadOrCreate(r5, 1 /* maxLen */)
	{
		require.False(t, loaded)
		require.Zero(t, q5.acc.Used())
		shouldQ, _, appended := q5.Append(e5, nil /* stream */)
		require.True(t, appended)
		require.True(t, shouldQ)

		// No accidental misattribution of bytes between the queues.
		require.Equal(t, 2*n1, q1.acc.Used())
		require.Equal(t, n5, q5.acc.Used())
	}

	// Delete the queue. Post deletion, even if someone still has a handle
	// to the deleted queue, the queue is empty and refuses appends. In other
	// words, we're not going to leak requests into abandoned queues.
	{
		qs.Delete(r1)
		shouldQ, _, appended := q1.Append(e1, nil /* stream */)
		require.False(t, appended)
		require.False(t, shouldQ)
		require.Zero(t, q1.acc.Used())
		require.Equal(t, n5, q5.acc.Used()) // we didn't touch q5
	}
}
