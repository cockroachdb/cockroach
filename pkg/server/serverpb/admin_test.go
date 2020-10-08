// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTableStatsResponseAdd verifies that TableStatsResponse.Add()
// correctly represents the result of combining stats from two spans.
// Specifically, most TableStatsResponse's stats are a straight-forward sum,
// but NodeCount should decrement as more missing nodes are added.
func TestTableStatsResponseAdd(t *testing.T) {

	// Initial object: no missing nodes.
	underTest := TableStatsResponse{
		RangeCount:           4,
		ReplicaCount:         4,
		ApproximateDiskBytes: 1000,
		NodeCount:            8,
	}

	// Add stats: no missing nodes, so NodeCount should stay the same.
	underTest.Add(&TableStatsResponse{
		RangeCount:           1,
		ReplicaCount:         2,
		ApproximateDiskBytes: 2345,
		NodeCount:            8,
	})
	assert.Equal(t, int64(5), underTest.RangeCount)
	assert.Equal(t, int64(6), underTest.ReplicaCount)
	assert.Equal(t, uint64(3345), underTest.ApproximateDiskBytes)
	assert.Equal(t, int64(8), underTest.NodeCount)

	// Add more stats: this time "node1" is missing. NodeCount should decrement.
	underTest.Add(&TableStatsResponse{
		RangeCount:           0,
		ReplicaCount:         0,
		ApproximateDiskBytes: 0,
		NodeCount:            7,
		MissingNodes: []TableStatsResponse_MissingNode{
			{
				NodeID:       "node1",
				ErrorMessage: "error msg",
			},
		},
	})
	assert.Equal(t, int64(5), underTest.RangeCount)
	assert.Equal(t, int64(6), underTest.ReplicaCount)
	assert.Equal(t, uint64(3345), underTest.ApproximateDiskBytes)
	assert.Equal(t, int64(7), underTest.NodeCount)
	assert.Equal(t, []TableStatsResponse_MissingNode{
		{
			NodeID:       "node1",
			ErrorMessage: "error msg",
		},
	}, underTest.MissingNodes)

	// Add more stats: "node1" is missing again. NodeCount shouldn't decrement.
	underTest.Add(&TableStatsResponse{
		RangeCount:           0,
		ReplicaCount:         0,
		ApproximateDiskBytes: 0,
		NodeCount:            7,
		MissingNodes: []TableStatsResponse_MissingNode{
			{
				NodeID:       "node1",
				ErrorMessage: "different error msg",
			},
		},
	})
	assert.Equal(t, int64(5), underTest.RangeCount)
	assert.Equal(t, int64(6), underTest.ReplicaCount)
	assert.Equal(t, uint64(3345), underTest.ApproximateDiskBytes)
	assert.Equal(t, int64(7), underTest.NodeCount)

	// Add more stats: new node is missing ("node2"). NodeCount should decrement.
	underTest.Add(&TableStatsResponse{
		RangeCount:           0,
		ReplicaCount:         0,
		ApproximateDiskBytes: 0,
		NodeCount:            7,
		MissingNodes: []TableStatsResponse_MissingNode{
			{
				NodeID:       "node2",
				ErrorMessage: "totally new error msg",
			},
		},
	})
	assert.Equal(t, int64(5), underTest.RangeCount)
	assert.Equal(t, int64(6), underTest.ReplicaCount)
	assert.Equal(t, uint64(3345), underTest.ApproximateDiskBytes)
	assert.Equal(t, int64(6), underTest.NodeCount)
	assert.Equal(t, []TableStatsResponse_MissingNode{
		{
			NodeID:       "node1",
			ErrorMessage: "error msg",
		},
		{
			NodeID:       "node2",
			ErrorMessage: "totally new error msg",
		},
	}, underTest.MissingNodes)

}
