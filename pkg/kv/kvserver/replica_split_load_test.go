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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func roachpbKey(key uint32) roachpb.Key {
	return keys.SystemSQLCodec.TablePrefix(key)
}

func requestHeaderWithNilEndKey(key uint32) roachpb.RequestHeader {
	return roachpb.RequestHeader{
		Key: roachpbKey(key),
	}
}

func requestHeader(key uint32, endKey uint32) roachpb.RequestHeader {
	return roachpb.RequestHeader{
		Key:    roachpbKey(key),
		EndKey: roachpbKey(endKey),
	}
}

func responseHeaderWithNilResumeSpan() roachpb.ResponseHeader {
	return roachpb.ResponseHeader{
		ResumeSpan: nil,
	}
}

func responseHeaderWithNilEndKey(key uint32) roachpb.ResponseHeader {
	return roachpb.ResponseHeader{
		ResumeSpan: &roachpb.Span{
			Key: roachpbKey(key),
		},
	}
}

func responseHeader(key uint32, endKey uint32) roachpb.ResponseHeader {
	return roachpb.ResponseHeader{
		ResumeSpan: &roachpb.Span{
			Key:    roachpbKey(key),
			EndKey: roachpbKey(endKey),
		},
	}
}

func requestUnionGet(requestHeader roachpb.RequestHeader) roachpb.RequestUnion {
	return roachpb.RequestUnion{
		Value: &roachpb.RequestUnion_Get{
			Get: &roachpb.GetRequest{
				RequestHeader: requestHeader,
			},
		},
	}
}

func responseUnionGet(responseHeader roachpb.ResponseHeader) roachpb.ResponseUnion {
	return roachpb.ResponseUnion{
		Value: &roachpb.ResponseUnion_Get{
			Get: &roachpb.GetResponse{
				ResponseHeader: responseHeader,
			},
		},
	}
}

func requestUnionScan(requestHeader roachpb.RequestHeader) roachpb.RequestUnion {
	return roachpb.RequestUnion{
		Value: &roachpb.RequestUnion_Scan{
			Scan: &roachpb.ScanRequest{
				RequestHeader: requestHeader,
			},
		},
	}
}

func responseUnionScan(responseHeader roachpb.ResponseHeader) roachpb.ResponseUnion {
	return roachpb.ResponseUnion{
		Value: &roachpb.ResponseUnion_Scan{
			Scan: &roachpb.ScanResponse{
				ResponseHeader: responseHeader,
			},
		},
	}
}

func requestUnionReverseScan(requestHeader roachpb.RequestHeader) roachpb.RequestUnion {
	return roachpb.RequestUnion{
		Value: &roachpb.RequestUnion_ReverseScan{
			ReverseScan: &roachpb.ReverseScanRequest{
				RequestHeader: requestHeader,
			},
		},
	}
}

func responseUnionReverseScan(responseHeader roachpb.ResponseHeader) roachpb.ResponseUnion {
	return roachpb.ResponseUnion{
		Value: &roachpb.ResponseUnion_ReverseScan{
			ReverseScan: &roachpb.ReverseScanResponse{
				ResponseHeader: responseHeader,
			},
		},
	}
}

func requestUnionDeleteRange(requestHeader roachpb.RequestHeader) roachpb.RequestUnion {
	return roachpb.RequestUnion{
		Value: &roachpb.RequestUnion_DeleteRange{
			DeleteRange: &roachpb.DeleteRangeRequest{
				RequestHeader: requestHeader,
			},
		},
	}
}

func responseUnionDeleteRange(responseHeader roachpb.ResponseHeader) roachpb.ResponseUnion {
	return roachpb.ResponseUnion{
		Value: &roachpb.ResponseUnion_DeleteRange{
			DeleteRange: &roachpb.DeleteRangeResponse{
				ResponseHeader: responseHeader,
			},
		},
	}
}

func TestGetResponseBoundarySpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		ba                           *roachpb.BatchRequest
		br                           *roachpb.BatchResponse
		expectedResponseBoundarySpan roachpb.Span
	}{
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionGet(requestHeaderWithNilEndKey(100)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					responseUnionGet(responseHeaderWithNilResumeSpan()),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key: roachpbKey(100),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionScan(requestHeader(100, 900)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					responseUnionScan(responseHeaderWithNilResumeSpan()),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(100),
				EndKey: roachpbKey(900),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionScan(requestHeader(100, 900)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					responseUnionScan(responseHeader(113, 900)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(100),
				EndKey: roachpbKey(113),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionReverseScan(requestHeader(100, 900)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					responseUnionReverseScan(responseHeader(100, 879)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(879),
				EndKey: roachpbKey(900),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionDeleteRange(requestHeader(100, 900)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					responseUnionDeleteRange(responseHeader(113, 900)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    roachpbKey(100),
				EndKey: roachpbKey(900),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionGet(requestHeaderWithNilEndKey(100)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					responseUnionGet(responseHeaderWithNilEndKey(100)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionScan(requestHeader(100, 900)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					responseUnionScan(responseHeader(100, 900)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionReverseScan(requestHeader(100, 900)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					responseUnionReverseScan(responseHeader(100, 900)),
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					requestUnionScan(requestHeader(500, 600)),
					requestUnionReverseScan(requestHeader(475, 625)),
					requestUnionGet(requestHeaderWithNilEndKey(480)),
					requestUnionReverseScan(requestHeader(500, 510)),
					requestUnionScan(requestHeader(700, 800)),
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
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
