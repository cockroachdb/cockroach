// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestDetectLaggingNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()

	detail := func(srcID, dstID, frontier int) frontierExecutionDetails {
		return frontierExecutionDetails{
			srcInstanceID:  base.SQLInstanceID(srcID),
			destInstanceID: base.SQLInstanceID(dstID),
			frontierTS:     hlc.Timestamp{WallTime: int64(frontier)},
		}
	}

	makeDetails := func(details ...frontierExecutionDetails) []frontierExecutionDetails {
		for i := len(details) - 1; i > 0; i-- {
			j := rng.Intn(i + 1)
			details[i], details[j] = details[j], details[i]
		}
		return details
	}

	for _, tc := range []struct {
		name             string
		details          []frontierExecutionDetails
		laggingNode      base.SQLInstanceID
		minLagDifference time.Duration
	}{
		{
			name: "simple",
			details: makeDetails(
				detail(1, 1, 3),
				detail(2, 2, 1),
				detail(3, 3, 6)),
			laggingNode:      2,
			minLagDifference: 2,
		},
		{
			name: "aggregateOnLagger",
			details: makeDetails(
				detail(1, 1, 4),
				detail(2, 2, 1),
				detail(3, 2, 5),
				detail(4, 3, 5)),
			laggingNode:      2,
			minLagDifference: 3,
		},
		{
			name: "aggregateOnBoth",
			details: makeDetails(
				detail(1, 1, 1),
				detail(2, 1, 6),
				detail(3, 2, 2),
				detail(4, 2, 4)),

			laggingNode:      1,
			minLagDifference: 1,
		},
		{
			name: "skipInitialScanNode",
			details: makeDetails(
				detail(1, 1, 0),
				detail(2, 2, 3),
				detail(3, 2, 2),
				detail(4, 3, 4)),

			laggingNode:      2,
			minLagDifference: 2,
		},
		{
			name: "laggingNodeMayHaveInitialScan",
			details: makeDetails(
				detail(1, 1, 0),
				detail(2, 1, 1),
				detail(3, 2, 3),
				detail(4, 3, 4)),

			laggingNode:      1,
			minLagDifference: 2,
		},
		{
			name: "singleNode",
			details: makeDetails(
				detail(1, 1, 1)),
			laggingNode:      0,
			minLagDifference: 0,
		},
		{
			name: "singleNodeMultiplePartitions",
			details: makeDetails(
				detail(1, 1, 1),
				detail(2, 1, 2),
			),
			laggingNode:      0,
			minLagDifference: 0,
		},
		{
			name:             "zeroNode",
			details:          makeDetails(),
			laggingNode:      0,
			minLagDifference: 0,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			laggingNode, minLagDifference := computeMinLagDifference(tc.details)
			require.Equal(t, tc.laggingNode, laggingNode)
			require.Equal(t, tc.minLagDifference, minLagDifference)
		})
	}
}

func TestCheckNewNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewTestRand()

	node := func(dstID int, startedAt int64) statuspb.NodeStatus {
		return statuspb.NodeStatus{
			Desc:      roachpb.NodeDescriptor{NodeID: roachpb.NodeID(dstID)},
			StartedAt: startedAt,
		}
	}

	makeNodes := func(node ...statuspb.NodeStatus) []statuspb.NodeStatus {
		for i := len(node) - 1; i > 0; i-- {
			j := rng.Intn(i + 1)
			node[i], node[j] = node[j], node[i]
		}
		return node
	}
	now := timeutil.Now().UnixNano()
	for _, tc := range []struct {
		name         string
		nodes        []statuspb.NodeStatus
		allowableLag int64
		laggingNode  int
		isNew        bool
	}{
		{
			name: "oneNewNode",
			nodes: makeNodes(
				node(1, now-10),
				node(2, 3),
				node(3, 4)),
			allowableLag: 5,
			laggingNode:  1,
			isNew:        true,
		},
		{
			name: "allOldNodes",
			nodes: makeNodes(
				node(1, 1),
				node(2, 2),
				node(3, 3)),
			allowableLag: 5,
			laggingNode:  1,
			isNew:        false,
		},
		{
			name: "nodeYoungerThanMaxAllowableLag",
			nodes: makeNodes(
				node(1, now-1),
				node(2, now-1),
				node(3, now-1)),
			allowableLag: time.Minute.Nanoseconds(),
			laggingNode:  1,
			isNew:        true,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.isNew, laggingNodeIsNew(base.SQLInstanceID(tc.laggingNode), time.Duration(tc.allowableLag), tc.nodes))
		})
	}
}
