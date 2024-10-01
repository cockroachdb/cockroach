// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostmodel

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestEstimatedCPUBatchInfo(t *testing.T) {
	for _, tc := range []struct {
		name     string
		request  kvpb.BatchRequest
		response kvpb.BatchResponse
		expected BatchInfo
	}{
		{
			name: "empty batch",
		},
		{
			name: "read-only batch",
			request: kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					{Value: &kvpb.RequestUnion_Get{
						Get: &kvpb.GetRequest{
							RequestHeader: kvpb.RequestHeader{Key: make([]byte, 10)},
						}}},
					{Value: &kvpb.RequestUnion_Get{
						Get: &kvpb.GetRequest{
							RequestHeader: kvpb.RequestHeader{Key: make([]byte, 20)},
						}}},
					{Value: &kvpb.RequestUnion_QueryIntent{
						QueryIntent: &kvpb.QueryIntentRequest{
							RequestHeader: kvpb.RequestHeader{Key: make([]byte, 30)},
						}}},
				},
			},
			response: kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					{Value: &kvpb.ResponseUnion_Get{
						Get: &kvpb.GetResponse{
							ResponseHeader: kvpb.ResponseHeader{NumBytes: 100},
						}}},
					{Value: &kvpb.ResponseUnion_Get{
						Get: &kvpb.GetResponse{
							ResponseHeader: kvpb.ResponseHeader{NumBytes: 200},
						}}},
					{Value: &kvpb.ResponseUnion_QueryIntent{
						QueryIntent: &kvpb.QueryIntentResponse{}}},
				},
			},
			expected: BatchInfo{
				ReadCount: 3,
				ReadBytes: 300,
			},
		},
		{
			name: "write-only batch",
			request: kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					{Value: &kvpb.RequestUnion_Put{
						Put: &kvpb.PutRequest{
							RequestHeader: kvpb.RequestHeader{Key: make([]byte, 10)},
							Value:         roachpb.Value{RawBytes: make([]byte, 100)}},
					}},
					{Value: &kvpb.RequestUnion_Put{
						Put: &kvpb.PutRequest{
							RequestHeader: kvpb.RequestHeader{Key: make([]byte, 20)},
							Value:         roachpb.Value{RawBytes: make([]byte, 200)}},
					}},
					{Value: &kvpb.RequestUnion_EndTxn{
						EndTxn: &kvpb.EndTxnRequest{
							RequestHeader: kvpb.RequestHeader{Key: make([]byte, 30)}},
					}},
				},
			},
			response: kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					{Value: &kvpb.ResponseUnion_Put{Put: &kvpb.PutResponse{}}},
					{Value: &kvpb.ResponseUnion_Put{Put: &kvpb.PutResponse{}}},
					{Value: &kvpb.ResponseUnion_EndTxn{EndTxn: &kvpb.EndTxnResponse{}}},
				},
			},
			expected: BatchInfo{
				WriteCount: 3,
				WriteBytes: 339,
			},
		},
		{
			name: "read-write batch",
			request: kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					{Value: &kvpb.RequestUnion_Put{
						Put: &kvpb.PutRequest{
							RequestHeader: kvpb.RequestHeader{Key: make([]byte, 10)},
							Value:         roachpb.Value{RawBytes: make([]byte, 100)}},
					}},
					{Value: &kvpb.RequestUnion_Get{
						Get: &kvpb.GetRequest{
							RequestHeader: kvpb.RequestHeader{Key: make([]byte, 20)},
						}}},
				},
			},
			response: kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					{Value: &kvpb.ResponseUnion_Put{Put: &kvpb.PutResponse{}}},
					{Value: &kvpb.ResponseUnion_Get{
						Get: &kvpb.GetResponse{
							ResponseHeader: kvpb.ResponseHeader{NumBytes: 200},
						}}},
				},
			},
			expected: BatchInfo{
				ReadCount:  1,
				ReadBytes:  200,
				WriteCount: 1,
				WriteBytes: 114,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := DefaultEstimatedCPUModel.MakeBatchInfo(&tc.request, &tc.response)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestEstimatedCPUBatchCost(t *testing.T) {
	model := EstimatedCPUModel{
		ReadBatchCost: 1,
		ReadRequestCost: struct {
			BatchSize     []float64
			CPUPerRequest []EstimatedCPU
		}{
			BatchSize:     []float64{8, 16, 32, 64},
			CPUPerRequest: []EstimatedCPU{0.5, 1.5, 2.5, 3.5},
		},
		ReadBytesCost: struct {
			PayloadSize []float64
			CPUPerByte  []EstimatedCPU
		}{
			PayloadSize: []float64{256, 1024, 4096},
			CPUPerByte:  []EstimatedCPU{1, 2, 3},
		},

		WriteBatchCost: struct {
			RatePerNode []float64
			CPUPerBatch []EstimatedCPU
		}{
			RatePerNode: []float64{100, 200, 400, 800},
			CPUPerBatch: []EstimatedCPU{0.5, 1, 1.5, 2},
		},
		WriteRequestCost: struct {
			BatchSize     []float64
			CPUPerRequest []EstimatedCPU
		}{
			BatchSize:     []float64{4, 8, 16, 32},
			CPUPerRequest: []EstimatedCPU{2, 3, 4, 5},
		},
		WriteBytesCost: struct {
			PayloadSize []float64
			CPUPerByte  []EstimatedCPU
		}{
			PayloadSize: []float64{256, 1024, 4096},
			CPUPerByte:  []EstimatedCPU{2, 4, 8},
		},
		BackgroundCPU: struct {
			Amount       EstimatedCPU
			Amortization float64
		}{
			Amount:       0.5,
			Amortization: 5,
		},
	}

	t.Run("roundtrip model through JSON", func(t *testing.T) {
		bytes, err := json.Marshal(model)
		require.NoError(t, err)

		var model2 EstimatedCPUModel
		require.NoError(t, json.Unmarshal(bytes, &model2))
		require.Equal(t, model, model2)
	})

	for _, tc := range []struct {
		name        string
		info        BatchInfo
		ratePerNode float64
		replicas    int64
		cost        EstimatedCPU
	}{
		{
			name: "read/write counts are 0",
			info: BatchInfo{
				ReadCount:  0,
				ReadBytes:  100,
				WriteCount: 0,
				WriteBytes: 1,
			},
			ratePerNode: 50,
			replicas:    3,
			cost:        0,
		},
		{
			name: "read/write counts are 1",
			info: BatchInfo{
				ReadCount:  1,
				ReadBytes:  100,
				WriteCount: 1,
				WriteBytes: 1,
			},
			ratePerNode: 50,
			replicas:    3,
			cost:        101 + 7.5,
		},
		{
			name: "metrics are smaller than min lookup values",
			info: BatchInfo{
				ReadCount:  3,
				ReadBytes:  100,
				WriteCount: 3,
				WriteBytes: 1,
			},
			ratePerNode: 50,
			replicas:    3,
			cost:        102 + 13.5,
		},
		{
			name: "metrics are equal to min lookup values",
			info: BatchInfo{
				ReadCount:  8,
				ReadBytes:  256,
				WriteCount: 4,
				WriteBytes: 256,
			},
			ratePerNode: 100,
			replicas:    3,
			cost:        260.5 + 1549.5,
		},
		{
			name: "metrics between lookup values",
			info: BatchInfo{
				ReadCount:  48,
				ReadBytes:  2560,
				WriteCount: 12,
				WriteBytes: 640,
			},
			ratePerNode: 150,
			replicas:    5,
			cost:        6542 + 9778.75,
		},
		{
			name: "metrics are equal to max lookup values",
			info: BatchInfo{
				ReadCount:  64,
				ReadBytes:  4096,
				WriteCount: 32,
				WriteBytes: 4096,
			},
			ratePerNode: 800,
			replicas:    3,
			cost:        12_509.5 + 98_760,
		},
		{
			name: "metrics are larger than max lookup values",
			info: BatchInfo{
				ReadCount:  1000,
				ReadBytes:  10000,
				WriteCount: 100,
				WriteBytes: 10000,
			},
			ratePerNode: 1000,
			replicas:    5,
			cost:        33_497.5 + 402_460,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.cost, model.BatchCost(tc.info, tc.ratePerNode, tc.replicas))
		})
	}
}
