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
					{
						Value: &roachpb.RequestUnion_Get{
							Get: &roachpb.GetRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: roachpb.KeyMin,
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_Get{
							Get: &roachpb.GetResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: nil,
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(100),
				EndKey: roachpb.KeyMin,
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{
						Value: &roachpb.RequestUnion_Scan{
							Scan: &roachpb.ScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(900),
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_Scan{
							Scan: &roachpb.ScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: nil,
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(100),
				EndKey: keys.SystemSQLCodec.TablePrefix(900),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{
						Value: &roachpb.RequestUnion_Scan{
							Scan: &roachpb.ScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(900),
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_Scan{
							Scan: &roachpb.ScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key:    keys.SystemSQLCodec.TablePrefix(113),
										EndKey: keys.SystemSQLCodec.TablePrefix(900),
									},
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(100),
				EndKey: keys.SystemSQLCodec.TablePrefix(113),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{
						Value: &roachpb.RequestUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(900),
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key:    keys.SystemSQLCodec.TablePrefix(100),
										EndKey: keys.SystemSQLCodec.TablePrefix(879),
									},
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(879),
				EndKey: keys.SystemSQLCodec.TablePrefix(900),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{
						Value: &roachpb.RequestUnion_DeleteRange{
							DeleteRange: &roachpb.DeleteRangeRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(900),
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_DeleteRange{
							DeleteRange: &roachpb.DeleteRangeResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key:    keys.SystemSQLCodec.TablePrefix(113),
										EndKey: keys.SystemSQLCodec.TablePrefix(900),
									},
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(100),
				EndKey: keys.SystemSQLCodec.TablePrefix(900),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{
						Value: &roachpb.RequestUnion_Get{
							Get: &roachpb.GetRequest{
								RequestHeader: roachpb.RequestHeader{
									Key: keys.SystemSQLCodec.TablePrefix(100),
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_Get{
							Get: &roachpb.GetResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key: keys.SystemSQLCodec.TablePrefix(100),
									},
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key: keys.SystemSQLCodec.TablePrefix(100),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{
						Value: &roachpb.RequestUnion_Scan{
							Scan: &roachpb.ScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(900),
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_Scan{
							Scan: &roachpb.ScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key:    keys.SystemSQLCodec.TablePrefix(100),
										EndKey: keys.SystemSQLCodec.TablePrefix(900),
									},
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(100),
				EndKey: keys.SystemSQLCodec.TablePrefix(100),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{
						Value: &roachpb.RequestUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(900),
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key:    keys.SystemSQLCodec.TablePrefix(100),
										EndKey: keys.SystemSQLCodec.TablePrefix(900),
									},
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(900),
				EndKey: keys.SystemSQLCodec.TablePrefix(900),
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{
						Value: &roachpb.RequestUnion_Scan{
							Scan: &roachpb.ScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(500),
									EndKey: keys.SystemSQLCodec.TablePrefix(600),
								},
							},
						},
					},
					{
						Value: &roachpb.RequestUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(475),
									EndKey: keys.SystemSQLCodec.TablePrefix(625),
								},
							},
						},
					},
					{
						Value: &roachpb.RequestUnion_Get{
							Get: &roachpb.GetRequest{
								RequestHeader: roachpb.RequestHeader{
									Key: keys.SystemSQLCodec.TablePrefix(480),
								},
							},
						},
					},
					{
						Value: &roachpb.RequestUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(500),
									EndKey: keys.SystemSQLCodec.TablePrefix(510),
								},
							},
						},
					},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{
						Value: &roachpb.ResponseUnion_Scan{
							Scan: &roachpb.ScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key:    keys.SystemSQLCodec.TablePrefix(550),
										EndKey: keys.SystemSQLCodec.TablePrefix(600),
									},
								},
							},
						},
					},
					{
						Value: &roachpb.ResponseUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key:    keys.SystemSQLCodec.TablePrefix(475),
										EndKey: keys.SystemSQLCodec.TablePrefix(525),
									},
								},
							},
						},
					},
					{
						Value: &roachpb.ResponseUnion_Get{
							Get: &roachpb.GetResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: nil,
								},
							},
						},
					},
					{
						Value: &roachpb.ResponseUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: nil,
								},
							},
						},
					},
				},
			},
			expectedResponseBoundarySpan: roachpb.Span{
				Key:    keys.SystemSQLCodec.TablePrefix(480),
				EndKey: keys.SystemSQLCodec.TablePrefix(625),
			},
		},
	}
	for i, test := range testCases {
		responseBoundarySpan := getResponseBoundarySpan(test.ba, test.br)
		assert.Equal(t, test.expectedResponseBoundarySpan, responseBoundarySpan, "Expected response boundary span %s, got %s in test %d",
			test.expectedResponseBoundarySpan, responseBoundarySpan, i)
	}
}
