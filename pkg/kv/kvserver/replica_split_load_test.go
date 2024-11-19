/// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestSplitPreCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sampleSpan := roachpb.RSpan{
		Key:    roachpb.RKey(roachpbKey(1)),
		EndKey: roachpb.RKey(roachpbKey(100)),
	}

	testCases := []struct {
		splitKey roachpb.Key
		span     roachpb.RSpan
		err      string
	}{
		{
			splitKey: roachpbKey(50),
			span:     sampleSpan,
			err:      "",
		},
		{
			splitKey: roachpbKey(1),
			span:     sampleSpan,
			err:      "split key is equal to range start key ",
		},
		{
			// Key is outside of range bounds in the next two tests, however this
			// isn't checked in the pre-check.
			splitKey: roachpbKey(0),
			span:     sampleSpan,
			err:      "",
		},
		{
			splitKey: roachpbKey(100),
			span:     sampleSpan,
			err:      "",
		},
		{
			// We also don't check that the returned key is a safe split key in the
			// pre-check.
			splitKey: keys.MakeFamilyKey(roachpbKey(50), 2),
			span:     sampleSpan,
			err:      "",
		},
		{
			splitKey: keys.MakeFamilyKey(roachpbKey(50), 0),
			span:     sampleSpan,
			err:      "",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			err := splitKeyPreCheck(tc.span, tc.splitKey)
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.err)
			}
		})
	}
}
