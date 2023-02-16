/// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func roachpbKey(key uint32) roachpb.Key {
	return keys.SystemSQLCodec.TablePrefix(key)
}

func requestHeaderWithNilEndKey(key uint32) kvpb.RequestHeader {
	return kvpb.RequestHeader{
		Key: roachpbKey(key),
	}
}

func requestHeader(key uint32, endKey uint32) kvpb.RequestHeader {
	return kvpb.RequestHeader{
		Key:    roachpbKey(key),
		EndKey: roachpbKey(endKey),
	}
}

func responseHeaderWithNilResumeSpan() kvpb.ResponseHeader {
	return kvpb.ResponseHeader{
		ResumeSpan: nil,
	}
}

func responseHeaderWithNilEndKey(key uint32) kvpb.ResponseHeader {
	return kvpb.ResponseHeader{
		ResumeSpan: &roachpb.Span{
			Key: roachpbKey(key),
		},
	}
}

func responseHeader(key uint32, endKey uint32) kvpb.ResponseHeader {
	return kvpb.ResponseHeader{
		ResumeSpan: &roachpb.Span{
			Key:    roachpbKey(key),
			EndKey: roachpbKey(endKey),
		},
	}
}

func requestUnionGet(requestHeader kvpb.RequestHeader) kvpb.RequestUnion {
	return kvpb.RequestUnion{
		Value: &kvpb.RequestUnion_Get{
			Get: &kvpb.GetRequest{
				RequestHeader: requestHeader,
			},
		},
	}
}

func responseUnionGet(responseHeader kvpb.ResponseHeader) kvpb.ResponseUnion {
	return kvpb.ResponseUnion{
		Value: &kvpb.ResponseUnion_Get{
			Get: &kvpb.GetResponse{
				ResponseHeader: responseHeader,
			},
		},
	}
}

func requestUnionScan(requestHeader kvpb.RequestHeader) kvpb.RequestUnion {
	return kvpb.RequestUnion{
		Value: &kvpb.RequestUnion_Scan{
			Scan: &kvpb.ScanRequest{
				RequestHeader: requestHeader,
			},
		},
	}
}

func responseUnionScan(responseHeader kvpb.ResponseHeader) kvpb.ResponseUnion {
	return kvpb.ResponseUnion{
		Value: &kvpb.ResponseUnion_Scan{
			Scan: &kvpb.ScanResponse{
				ResponseHeader: responseHeader,
			},
		},
	}
}

func requestUnionReverseScan(requestHeader kvpb.RequestHeader) kvpb.RequestUnion {
	return kvpb.RequestUnion{
		Value: &kvpb.RequestUnion_ReverseScan{
			ReverseScan: &kvpb.ReverseScanRequest{
				RequestHeader: requestHeader,
			},
		},
	}
}

func responseUnionReverseScan(responseHeader kvpb.ResponseHeader) kvpb.ResponseUnion {
	return kvpb.ResponseUnion{
		Value: &kvpb.ResponseUnion_ReverseScan{
			ReverseScan: &kvpb.ReverseScanResponse{
				ResponseHeader: responseHeader,
			},
		},
	}
}

func requestUnionDeleteRange(requestHeader kvpb.RequestHeader) kvpb.RequestUnion {
	return kvpb.RequestUnion{
		Value: &kvpb.RequestUnion_DeleteRange{
			DeleteRange: &kvpb.DeleteRangeRequest{
				RequestHeader: requestHeader,
			},
		},
	}
}

func responseUnionDeleteRange(responseHeader kvpb.ResponseHeader) kvpb.ResponseUnion {
	return kvpb.ResponseUnion{
		Value: &kvpb.ResponseUnion_DeleteRange{
			DeleteRange: &kvpb.DeleteRangeResponse{
				ResponseHeader: responseHeader,
			},
		},
	}
}

func TestGetResponseBoundarySpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		ba                           *kvpb.BatchRequest
		br                           *kvpb.BatchResponse
		expectedResponseBoundarySpan roachpb.Span
	}{
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionGet(requestHeaderWithNilEndKey(100)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionGet(responseHeaderWithNilResumeSpan()),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key: roachpbKey(100),
			},
		},
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionScan(requestHeader(100, 900)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionScan(responseHeaderWithNilResumeSpan()),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(100),
				EndKey: roachpbKey(900),
			},
		},
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionScan(requestHeader(100, 900)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionScan(responseHeader(113, 900)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(100),
				EndKey: roachpbKey(113),
			},
		},
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionReverseScan(requestHeader(100, 900)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionReverseScan(responseHeader(100, 879)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(879),
				EndKey: roachpbKey(900),
			},
		},
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionDeleteRange(requestHeader(100, 900)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionDeleteRange(responseHeader(113, 900)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(100),
				EndKey: roachpbKey(900),
			},
		},
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionGet(requestHeaderWithNilEndKey(100)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionGet(responseHeaderWithNilEndKey(100)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{},
		},
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionScan(requestHeader(100, 900)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionScan(responseHeader(100, 900)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{},
		},
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionReverseScan(requestHeader(100, 900)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionReverseScan(responseHeader(100, 900)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{},
		},
		{
			ba: &kvpb.BatchRequest{
				Requests: []kvpb.RequestUnion{
					requestUnionScan(requestHeader(500, 600)),
					requestUnionReverseScan(requestHeader(475, 625)),
					requestUnionGet(requestHeaderWithNilEndKey(480)),
					requestUnionReverseScan(requestHeader(500, 510)),
					requestUnionScan(requestHeader(700, 800)),
				},
			},
			br: &kvpb.BatchResponse{
				Responses: []kvpb.ResponseUnion{
					responseUnionScan(responseHeader(550, 600)),
					responseUnionReverseScan(responseHeader(475, 525)),
					responseUnionGet(responseHeaderWithNilResumeSpan()),
					responseUnionReverseScan(responseHeaderWithNilResumeSpan()),
					responseUnionScan(responseHeader(700, 800)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(480),
				EndKey: roachpbKey(625),
			},
		},
	}
	for i, test := range testCases {
		responseBoundarySpan := getResponseBoundarySpan(test.ba, test.br)
		assert.Equal(t, test.expectedResponseBoundarySpan, responseBoundarySpan, "Expected response boundary span %s, got %s in test %d",
			test.expectedResponseBoundarySpan, responseBoundarySpan, i)
	}
}
