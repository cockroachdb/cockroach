// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostmodel

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

func TestRequestUnitBatchInfo(t *testing.T) {
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
				ReadCount: 2,
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
				WriteCount: 2,
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
			var ruModel RequestUnitModel
			actual := ruModel.MakeBatchInfo(&tc.request, &tc.response)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestRequestUnitBatchCost(t *testing.T) {
	model := RequestUnitModel{
		KVReadBatch:    1,
		KVReadRequest:  0.5,
		KVReadByte:     0.1,
		KVWriteBatch:   10,
		KVWriteRequest: 5,
		KVWriteByte:    0.2,
	}

	for _, tc := range []struct {
		name        string
		info        BatchInfo
		networkCost NetworkCost
		replicas    int64
		networkRU   RU
		kvRU        RU
	}{
		{
			name: "read/write counts are 0",
			info: BatchInfo{
				ReadCount:  0,
				ReadBytes:  100,
				WriteCount: 0,
				WriteBytes: 1,
			},
			replicas:  3,
			kvRU:      0,
			networkRU: 0,
		},
		{
			name: "read batch",
			info: BatchInfo{
				ReadCount: 10,
				ReadBytes: 100,
			},
			networkCost: 0.1,
			kvRU:        16,
			networkRU:   10,
		},
		{
			name: "write batch",
			info: BatchInfo{
				ReadCount:  10,
				ReadBytes:  100,
				WriteCount: 5,
				WriteBytes: 50,
			},
			replicas:    3,
			networkCost: 2,
			kvRU:        135,
			networkRU:   100,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvRU, networkRU := model.BatchCost(tc.info, tc.networkCost, tc.replicas)
			require.Equal(t, tc.kvRU, kvRU)
			require.Equal(t, tc.networkRU, networkRU)
		})
	}
}
