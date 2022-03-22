// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// mockServerStream is an implementation of grpc.ServerStream that receives a
// list of integers.
type mockServerStream []int

func (mockServerStream) SetHeader(metadata.MD) error  { panic("unimplemented") }
func (mockServerStream) SendHeader(metadata.MD) error { panic("unimplemented") }
func (mockServerStream) SetTrailer(metadata.MD)       { panic("unimplemented") }
func (mockServerStream) Context() context.Context     { panic("unimplemented") }
func (mockServerStream) SendMsg(m interface{}) error  { panic("unimplemented") }
func (s *mockServerStream) RecvMsg(m interface{}) error {
	if len(*s) == 0 {
		return io.EOF
	}
	*(m.(*int)) = (*s)[0]
	*s = (*s)[1:]
	return nil
}

func TestWrappedServerStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ss := mockServerStream{1, 2, 3}
	ctx := context.WithValue(context.Background(), struct{}{}, "v")

	var recv int
	wrapped := &wrappedServerStream{
		ServerStream: &ss,
		ctx:          ctx,
		recv: func(m interface{}) error {
			if err := ss.RecvMsg(m); err != nil {
				return err
			}
			recv = *(m.(*int))
			return nil
		},
	}

	// Context() returns the wrapped context.
	require.Equal(t, ctx, wrapped.Context())

	// RecvMsg calls the instrumented function.
	var i int
	require.NoError(t, wrapped.RecvMsg(&i))
	require.Equal(t, 1, i)
	require.Equal(t, 1, recv)

	// The wrapped stream can be used directly.
	require.NoError(t, wrapped.ServerStream.RecvMsg(&i))
	require.Equal(t, 2, i)
	require.Equal(t, 1, recv)

	require.NoError(t, wrapped.RecvMsg(&i))
	require.Equal(t, 3, i)
	require.Equal(t, 3, recv)

	// io.EOF propagated correctly. Message not updated.
	require.Equal(t, io.EOF, wrapped.RecvMsg(&i))
	require.Equal(t, 3, i)
	require.Equal(t, 3, recv)
}

func TestTenantFromCert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	correctOU := []string{security.TenantsOU}
	for _, tc := range []struct {
		ous        []string
		commonName string
		expTenID   roachpb.TenantID
		expErr     string
	}{
		{ous: correctOU, commonName: "10", expTenID: roachpb.MakeTenantID(10)},
		{ous: correctOU, commonName: roachpb.MinTenantID.String(), expTenID: roachpb.MinTenantID},
		{ous: correctOU, commonName: roachpb.MaxTenantID.String(), expTenID: roachpb.MaxTenantID},
		{ous: correctOU, commonName: roachpb.SystemTenantID.String() /* "system" */, expErr: `could not parse tenant ID from Common Name \(CN\)`},
		{ous: correctOU, commonName: "-1", expErr: `could not parse tenant ID from Common Name \(CN\)`},
		{ous: correctOU, commonName: "0", expErr: `invalid tenant ID 0 in Common Name \(CN\)`},
		{ous: correctOU, commonName: "1", expErr: `invalid tenant ID 1 in Common Name \(CN\)`},
		{ous: correctOU, commonName: "root", expErr: `could not parse tenant ID from Common Name \(CN\)`},
		{ous: correctOU, commonName: "other", expErr: `could not parse tenant ID from Common Name \(CN\)`},
		{ous: []string{"foo"}, commonName: "other", expErr: `user \[other\] is not allowed to perform this RPC`},
		{ous: nil, commonName: "other", expErr: `user \[other\] is not allowed to perform this RPC`},
		{ous: append([]string{"foo"}, correctOU...), commonName: "other", expErr: `could not parse tenant ID from Common Name`},
	} {
		t.Run(tc.commonName, func(t *testing.T) {
			cert := &x509.Certificate{
				Subject: pkix.Name{
					CommonName:         tc.commonName,
					OrganizationalUnit: tc.ous,
				},
			}
			tlsInfo := credentials.TLSInfo{
				State: tls.ConnectionState{
					PeerCertificates: []*x509.Certificate{cert},
				},
			}
			p := peer.Peer{AuthInfo: tlsInfo}
			ctx := peer.NewContext(context.Background(), &p)

			tenID, err := kvAuth{tenant: tenantAuthorizer{tenantID: roachpb.SystemTenantID}}.authenticate(ctx)

			if tc.expErr == "" {
				require.Equal(t, tc.expTenID, tenID)
				require.NoError(t, err)
			} else {
				require.Zero(t, tenID)
				require.Error(t, err)
				require.Equal(t, codes.Unauthenticated, status.Code(err))
				require.Regexp(t, tc.expErr, err)
			}
		})
	}
}

func TestTenantAuthRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tenID := roachpb.MakeTenantID(10)
	prefix := func(tenID uint64, key string) string {
		tenPrefix := keys.MakeTenantPrefix(roachpb.MakeTenantID(tenID))
		return string(append(tenPrefix, []byte(key)...))
	}
	makeSpan := func(key string, endKey ...string) roachpb.Span {
		s := roachpb.Span{Key: roachpb.Key(key)}
		if len(endKey) > 1 {
			t.Fatalf("unexpected endKey vararg %v", endKey)
		} else if len(endKey) == 1 {
			s.EndKey = roachpb.Key(endKey[0])
		}
		return s
	}
	makeReq := func(key string, endKey ...string) roachpb.Request {
		s := makeSpan(key, endKey...)
		h := roachpb.RequestHeaderFromSpan(s)
		return &roachpb.ScanRequest{RequestHeader: h}
	}
	makeDisallowedAdminReq := func(key string) roachpb.Request {
		s := makeSpan(key)
		h := roachpb.RequestHeader{Key: s.Key}
		return &roachpb.AdminUnsplitRequest{RequestHeader: h}
	}
	makeAdminSplitReq := func(key string) roachpb.Request {
		s := makeSpan(key)
		h := roachpb.RequestHeaderFromSpan(s)
		return &roachpb.AdminSplitRequest{RequestHeader: h, SplitKey: s.Key}
	}
	makeAdminScatterReq := func(key string) roachpb.Request {
		s := makeSpan(key)
		h := roachpb.RequestHeaderFromSpan(s)
		return &roachpb.AdminScatterRequest{RequestHeader: h}
	}
	makeReqs := func(reqs ...roachpb.Request) []roachpb.RequestUnion {
		ru := make([]roachpb.RequestUnion, len(reqs))
		for i, r := range reqs {
			ru[i].MustSetInner(r)
		}
		return ru
	}
	makeSystemSpanConfigTarget := func(source, target uint64) roachpb.SpanConfigTarget {
		return roachpb.SpanConfigTarget{
			Union: &roachpb.SpanConfigTarget_SystemSpanConfigTarget{
				SystemSpanConfigTarget: &roachpb.SystemSpanConfigTarget{
					SourceTenantID: roachpb.MakeTenantID(source),
					Type:           roachpb.NewSpecificTenantKeyspaceTargetType(roachpb.MakeTenantID(target)),
				},
			},
		}
	}
	makeSpanTarget := func(sp roachpb.Span) roachpb.SpanConfigTarget {
		return spanconfig.MakeTargetFromSpan(sp).ToProto()
	}
	makeGetSpanConfigsReq := func(
		target roachpb.SpanConfigTarget,
	) *roachpb.GetSpanConfigsRequest {
		return &roachpb.GetSpanConfigsRequest{Targets: []roachpb.SpanConfigTarget{target}}
	}
	makeUpdateSpanConfigsReq := func(target roachpb.SpanConfigTarget, delete bool) *roachpb.UpdateSpanConfigsRequest {
		if delete {
			return &roachpb.UpdateSpanConfigsRequest{ToDelete: []roachpb.SpanConfigTarget{target}}
		}
		return &roachpb.UpdateSpanConfigsRequest{ToUpsert: []roachpb.SpanConfigEntry{
			{
				Target: target,
			},
		}}
	}

	const noError = ""
	for method, tests := range map[string][]struct {
		req    interface{}
		expErr string
	}{
		"/cockroach.roachpb.Internal/Batch": {
			{
				req:    &roachpb.BatchRequest{},
				expErr: `requested key span /Max not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq("a", "b"),
				)},
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(5, "a"), prefix(5, "b")),
				)},
				expErr: `requested key span /Tenant/5"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: noError,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(50, "a"), prefix(50, "b")),
				)},
				expErr: `requested key span /Tenant/50"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq("a", "b"),
					makeReq(prefix(5, "a"), prefix(5, "b")),
				)},
				expErr: `requested key span {a-/Tenant/5"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(5, "a"), prefix(5, "b")),
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: `requested key span /Tenant/{5"a"-10"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq("a", prefix(10, "b")),
				)},
				expErr: `requested key span {a-/Tenant/10"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(20, "b")),
				)},
				expErr: `requested key span /Tenant/{10"a"-20"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeDisallowedAdminReq("a"),
				)},
				expErr: `request \[1 AdmUnsplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeDisallowedAdminReq(prefix(10, "a")),
				)},
				expErr: `request \[1 AdmUnsplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeDisallowedAdminReq(prefix(50, "a")),
				)},
				expErr: `request \[1 AdmUnsplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeDisallowedAdminReq(prefix(10, "a")),
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: `request \[1 Scan, 1 AdmUnsplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
					makeDisallowedAdminReq(prefix(10, "a")),
				)},
				expErr: `request \[1 Scan, 1 AdmUnsplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminSplitReq("a"),
				)},
				expErr: `requested key span a{-\\x00} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminSplitReq(prefix(10, "a")),
				)},
				expErr: noError,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminSplitReq(prefix(50, "a")),
				)},
				expErr: `requested key span /Tenant/50"a{"-\\x00"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminSplitReq(prefix(10, "a")),
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: noError,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
					makeAdminSplitReq(prefix(10, "a")),
				)},
				expErr: noError,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminScatterReq("a"),
				)},
				expErr: `requested key span a{-\\x00} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminScatterReq(prefix(10, "a")),
				)},
				expErr: noError,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminScatterReq(prefix(50, "a")),
				)},
				expErr: `requested key span /Tenant/50"a{"-\\x00"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminScatterReq(prefix(10, "a")),
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: noError,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
					makeAdminScatterReq(prefix(10, "a")),
				)},
				expErr: noError,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					func() roachpb.Request {
						h := roachpb.RequestHeaderFromSpan(makeSpan("a"))
						return &roachpb.SubsumeRequest{RequestHeader: h}
					}(),
				)},
				expErr: `request \[1 Subsume\] not permitted`,
			},
		},
		"/cockroach.roachpb.Internal/RangeLookup": {
			{
				req:    &roachpb.RangeLookupRequest{},
				expErr: `requested key /Min not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &roachpb.RangeLookupRequest{Key: roachpb.RKey("a")},
				expErr: `requested key "a" not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &roachpb.RangeLookupRequest{Key: roachpb.RKey(prefix(5, "a"))},
				expErr: `requested key /Tenant/5"a" not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &roachpb.RangeLookupRequest{Key: roachpb.RKey(prefix(10, "a"))},
				expErr: noError,
			},
			{
				req:    &roachpb.RangeLookupRequest{Key: roachpb.RKey(prefix(50, "a"))},
				expErr: `requested key /Tenant/50"a" not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
		},
		"/cockroach.roachpb.Internal/RangeFeed": {
			{
				req:    &roachpb.RangeFeedRequest{},
				expErr: `requested key span /Min not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &roachpb.RangeFeedRequest{Span: makeSpan("a", "b")},
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &roachpb.RangeFeedRequest{Span: makeSpan(prefix(5, "a"), prefix(5, "b"))},
				expErr: `requested key span /Tenant/5"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &roachpb.RangeFeedRequest{Span: makeSpan(prefix(10, "a"), prefix(10, "b"))},
				expErr: noError,
			},
			{
				req:    &roachpb.RangeFeedRequest{Span: makeSpan(prefix(50, "a"), prefix(50, "b"))},
				expErr: `requested key span /Tenant/50"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &roachpb.RangeFeedRequest{Span: makeSpan("a", prefix(10, "b"))},
				expErr: `requested key span {a-/Tenant/10"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &roachpb.RangeFeedRequest{Span: makeSpan(prefix(10, "a"), prefix(20, "b"))},
				expErr: `requested key span /Tenant/{10"a"-20"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
		},
		"/cockroach.roachpb.Internal/GossipSubscription": {
			{
				req:    &roachpb.GossipSubscriptionRequest{},
				expErr: noError,
			},
			{
				req:    &roachpb.GossipSubscriptionRequest{Patterns: []string{"node:.*"}},
				expErr: noError,
			},
			{
				req:    &roachpb.GossipSubscriptionRequest{Patterns: []string{"system-db"}},
				expErr: noError,
			},
			{
				req:    &roachpb.GossipSubscriptionRequest{Patterns: []string{"table-stat-added"}},
				expErr: `requested pattern "table-stat-added" not permitted`,
			},
			{
				req:    &roachpb.GossipSubscriptionRequest{Patterns: []string{"node:.*", "system-db"}},
				expErr: noError,
			},
			{
				req:    &roachpb.GossipSubscriptionRequest{Patterns: []string{"node:.*", "system-db", "table-stat-added"}},
				expErr: `requested pattern "table-stat-added" not permitted`,
			},
		},
		"/cockroach.roachpb.Internal/TokenBucket": {
			{
				req:    &roachpb.TokenBucketRequest{TenantID: tenID.ToUint64()},
				expErr: noError,
			},
			{
				req:    &roachpb.TokenBucketRequest{TenantID: roachpb.SystemTenantID.ToUint64()},
				expErr: `token bucket request for tenant system not permitted`,
			},
			{
				req:    &roachpb.TokenBucketRequest{TenantID: 13},
				expErr: `token bucket request for tenant 13 not permitted`,
			},
			{
				req:    &roachpb.TokenBucketRequest{},
				expErr: `token bucket request with unspecified tenant not permitted`,
			},
		},
		"/cockroach.roachpb.Internal/GetSpanConfigs": {
			{
				req:    &roachpb.GetSpanConfigsRequest{},
				expErr: noError,
			},
			{
				req:    makeGetSpanConfigsReq(makeSpanTarget(makeSpan("a", "b"))),
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeGetSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(5, "a"), prefix(5, "b"))),
				),
				expErr: `requested key span /Tenant/5"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeGetSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(10, "b"))),
				),
				expErr: noError,
			},
			{
				req: makeGetSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(50, "a"), prefix(50, "b"))),
				),
				expErr: `requested key span /Tenant/50"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeGetSpanConfigsReq(
					makeSpanTarget(makeSpan("a", prefix(10, "b"))),
				),
				expErr: `requested key span {a-/Tenant/10"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeGetSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(20, "b"))),
				),
				expErr: `requested key span /Tenant/{10"a"-20"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeGetSpanConfigsReq(makeSystemSpanConfigTarget(10, 10)),
				expErr: noError,
			},
			{
				req:    makeGetSpanConfigsReq(makeSystemSpanConfigTarget(10, 20)),
				expErr: `secondary tenants cannot interact with system span configurations of other tenants`,
			},
			{
				// Ensure tenant 10 (the tenant we test all these with) can't pretend
				// to be tenant 20 to get access to system span configurations.
				req:    makeGetSpanConfigsReq(makeSystemSpanConfigTarget(20, 20)),
				expErr: `malformed source tenant field`,
			},
			{
				req: makeGetSpanConfigsReq(roachpb.SpanConfigTarget{
					Union: &roachpb.SpanConfigTarget_SystemSpanConfigTarget{
						SystemSpanConfigTarget: &roachpb.SystemSpanConfigTarget{
							SourceTenantID: roachpb.MakeTenantID(10),
							Type:           roachpb.NewEntireKeyspaceTargetType(),
						},
					},
				}),
				expErr: `secondary tenants cannot target the entire keyspace`,
			},
			{
				req: makeGetSpanConfigsReq(roachpb.SpanConfigTarget{
					Union: &roachpb.SpanConfigTarget_SystemSpanConfigTarget{
						SystemSpanConfigTarget: &roachpb.SystemSpanConfigTarget{
							SourceTenantID: roachpb.MakeTenantID(20),
							Type:           roachpb.NewEntireKeyspaceTargetType(),
						},
					},
				}),
				expErr: `malformed source tenant field`,
			},
		},
		"/cockroach.roachpb.Internal/UpdateSpanConfigs": {
			{
				req:    &roachpb.UpdateSpanConfigsRequest{},
				expErr: noError,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan("a", "b")),
					true,
				),
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(5, "a"), prefix(5, "b"))),
					true,
				),
				expErr: `requested key span /Tenant/5"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(10, "b"))),
					true,
				),
				expErr: noError,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(50, "a"), prefix(50, "b"))),
					true,
				),
				expErr: `requested key span /Tenant/50"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan("a", prefix(10, "b"))),
					true,
				),
				expErr: `requested key span {a-/Tenant/10"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(20, "b"))),
					true,
				),
				expErr: `requested key span /Tenant/{10"a"-20"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan("a", "b")),
					false,
				),
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(5, "a"), prefix(5, "b"))),
					false,
				),
				expErr: `requested key span /Tenant/5"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(10, "b"))),
					false,
				),
				expErr: noError,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(50, "a"), prefix(50, "b"))),
					false,
				),
				expErr: `requested key span /Tenant/50"{a"-b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan("a", prefix(10, "b"))),
					false,
				),
				expErr: `requested key span {a-/Tenant/10"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(20, "b"))),
					false,
				),
				expErr: `requested key span /Tenant/{10"a"-20"b"} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeUpdateSpanConfigsReq(makeSystemSpanConfigTarget(10, 10), false),
				expErr: noError,
			},
			{
				req:    makeUpdateSpanConfigsReq(makeSystemSpanConfigTarget(10, 20), false),
				expErr: `secondary tenants cannot interact with system span configurations of other tenants`,
			},
			{
				// Ensure tenant 10 (the tenant we test all these with) can't pretend
				// to be tenant 20 to get access to system span configurations.
				req:    makeUpdateSpanConfigsReq(makeSystemSpanConfigTarget(20, 20), false),
				expErr: `malformed source tenant field`,
			},
			{
				req: makeUpdateSpanConfigsReq(roachpb.SpanConfigTarget{
					Union: &roachpb.SpanConfigTarget_SystemSpanConfigTarget{
						SystemSpanConfigTarget: &roachpb.SystemSpanConfigTarget{
							SourceTenantID: roachpb.MakeTenantID(10),
							Type:           roachpb.NewEntireKeyspaceTargetType(),
						},
					},
				}, false),
				expErr: `secondary tenants cannot target the entire keyspace`,
			},
			{
				req:    makeUpdateSpanConfigsReq(makeSystemSpanConfigTarget(10, 10), true),
				expErr: noError,
			},
			{
				req:    makeUpdateSpanConfigsReq(makeSystemSpanConfigTarget(10, 20), true),
				expErr: `secondary tenants cannot interact with system span configurations of other tenants`,
			},
			{
				// Ensure tenant 10 (the tenant we test all these with) can't pretend
				// to be tenant 20 to get access to system span configurations.
				req:    makeUpdateSpanConfigsReq(makeSystemSpanConfigTarget(20, 20), true),
				expErr: `malformed source tenant field`,
			},
			{
				req: makeUpdateSpanConfigsReq(roachpb.SpanConfigTarget{
					Union: &roachpb.SpanConfigTarget_SystemSpanConfigTarget{
						SystemSpanConfigTarget: &roachpb.SystemSpanConfigTarget{
							SourceTenantID: roachpb.MakeTenantID(10),
							Type:           roachpb.NewEntireKeyspaceTargetType(),
						},
					},
				}, true),
				expErr: `secondary tenants cannot target the entire keyspace`,
			},
		},

		"/cockroach.rpc.Heartbeat/Ping": {
			{req: &PingRequest{}, expErr: noError},
		},
		"/cockroach.rpc.Testing/Foo": {
			{req: "req", expErr: `unknown method "/cockroach.rpc.Testing/Foo"`},
		},
	} {
		t.Run(method, func(t *testing.T) {
			for _, tc := range tests {
				t.Run("", func(t *testing.T) {
					err := tenantAuthorizer{}.authorize(tenID, method, tc.req)
					if tc.expErr == noError {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
						require.Equal(t, codes.Unauthenticated, status.Code(err))
						require.Regexp(t, tc.expErr, err)
					}
				})
			}
		})
	}
}
