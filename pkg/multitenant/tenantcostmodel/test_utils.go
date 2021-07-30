// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcostmodel

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TestingRequestInfo creates a RequestInfo for testing purposes.
func TestingRequestInfo(isWrite bool, writeBytes int64) RequestInfo {
	if !isWrite {
		return RequestInfo{writeBytes: -1}
	}
	return RequestInfo{writeBytes: writeBytes}
}

// MockBatchRequest creates a mock BatchRequest from which MakeRequestInfo will
// extract the given information.
func MockBatchRequest(isWrite bool, writeBytes int64) *roachpb.BatchRequest {
	if !isWrite {
		return &roachpb.BatchRequest{
			Requests: []roachpb.RequestUnion{{
				Value: &roachpb.RequestUnion_Scan{
					Scan: &roachpb.ScanRequest{},
				},
			}},
		}
	}
	var emptyValue roachpb.Value
	emptyValueSize := emptyValue.Size()
	if writeBytes < int64(emptyValueSize) {
		panic(fmt.Sprintf("writeBytes must be at least %d", emptyValueSize))
	}
	writeBytes -= int64(emptyValueSize)

	return &roachpb.BatchRequest{
		Requests: []roachpb.RequestUnion{{
			Value: &roachpb.RequestUnion_Put{
				Put: &roachpb.PutRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: make(roachpb.Key, int(writeBytes)),
					},
				},
			},
		}},
	}
}

// TestingResponseInfo creates a ResponseInfo for testing purposes.
func TestingResponseInfo(readBytes int64) ResponseInfo {
	return ResponseInfo{readBytes: readBytes}
}

// MockBatchResponse creates a mock BatchResponse from which MakeResponseInfo
// will extract the given information.
func MockBatchResponse(readBytes int64) *roachpb.BatchResponse {
	if readBytes == 0 {
		return &roachpb.BatchResponse{}
	}
	return &roachpb.BatchResponse{
		Responses: []roachpb.ResponseUnion{{
			Value: &roachpb.ResponseUnion_Scan{
				Scan: &roachpb.ScanResponse{
					ResponseHeader: roachpb.ResponseHeader{
						NumBytes: readBytes,
					},
				},
			},
		}},
	}
}
