// Copyright 2022 The Cockroach Authors.
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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestReplicaGetTrueSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		ba                *roachpb.BatchRequest
		br                *roachpb.BatchResponse
		expectedTrueSpans []roachpb.Span
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
					{
						Value: &roachpb.RequestUnion_Scan{
							Scan: &roachpb.ScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(1000),
								},
							},
						},
					},
					{
						Value: &roachpb.RequestUnion_Scan{
							Scan: &roachpb.ScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(1000),
								},
							},
						},
					},
					{
						Value: &roachpb.RequestUnion_ReverseScan{
							ReverseScan: &roachpb.ReverseScanRequest{
								RequestHeader: roachpb.RequestHeader{
									Key:    keys.SystemSQLCodec.TablePrefix(100),
									EndKey: keys.SystemSQLCodec.TablePrefix(1000),
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
					{
						Value: &roachpb.ResponseUnion_Scan{
							Scan: &roachpb.ScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: nil,
								},
							},
						},
					},
					{
						Value: &roachpb.ResponseUnion_Scan{
							Scan: &roachpb.ScanResponse{
								ResponseHeader: roachpb.ResponseHeader{
									ResumeSpan: &roachpb.Span{
										Key:    keys.SystemSQLCodec.TablePrefix(113),
										EndKey: keys.SystemSQLCodec.TablePrefix(1000),
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
										Key:    keys.SystemSQLCodec.TablePrefix(100),
										EndKey: keys.SystemSQLCodec.TablePrefix(979),
									},
								},
							},
						},
					},
				},
			},
			expectedTrueSpans: []roachpb.Span{
				{
					Key:    keys.SystemSQLCodec.TablePrefix(100),
					EndKey: roachpb.KeyMin,
				},
				{
					Key:    keys.SystemSQLCodec.TablePrefix(100),
					EndKey: keys.SystemSQLCodec.TablePrefix(1000),
				},
				{
					Key:    keys.SystemSQLCodec.TablePrefix(100),
					EndKey: keys.SystemSQLCodec.TablePrefix(113),
				},
				{
					Key:    keys.SystemSQLCodec.TablePrefix(979),
					EndKey: keys.SystemSQLCodec.TablePrefix(1000),
				},
			},
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{
					{},
					{},
				},
			},
			br: &roachpb.BatchResponse{
				Responses: []roachpb.ResponseUnion{
					{},
				},
			},
			expectedTrueSpans: nil,
		},
		{
			ba: &roachpb.BatchRequest{
				Requests: []roachpb.RequestUnion{},
			},
			br:                nil,
			expectedTrueSpans: nil,
		},
	}
	for i, test := range testCases {
		trueSpanSet := getTrueSpans(context.Background(), test.ba, test.br)
		var trueSpans []roachpb.Span
		trueSpanSet.Iterate(func(spanAccess spanset.SpanAccess, spanScope spanset.SpanScope, span spanset.Span) {
			trueSpans = append(trueSpans, span.Span)
		})
		assert.Equal(t, test.expectedTrueSpans, trueSpans, "True spans not equal in test %d", i)
	}
}
