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

			tenID, err := kvAuth{}.authenticate(ctx)

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
	makeAdminReq := func(key string) roachpb.Request {
		s := makeSpan(key)
		h := roachpb.RequestHeaderFromSpan(s)
		return &roachpb.AdminSplitRequest{RequestHeader: h, SplitKey: s.Key}
	}
	makeReqs := func(reqs ...roachpb.Request) []roachpb.RequestUnion {
		ru := make([]roachpb.RequestUnion, len(reqs))
		for i, r := range reqs {
			ru[i].MustSetInner(r)
		}
		return ru
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
					makeAdminReq("a"),
				)},
				expErr: `request \[1 AdmSplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminReq(prefix(10, "a")),
				)},
				expErr: `request \[1 AdmSplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminReq(prefix(50, "a")),
				)},
				expErr: `request \[1 AdmSplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeAdminReq(prefix(10, "a")),
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: `request \[1 Scan, 1 AdmSplit\] not permitted`,
			},
			{
				req: &roachpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
					makeAdminReq(prefix(10, "a")),
				)},
				expErr: `request \[1 Scan, 1 AdmSplit\] not permitted`,
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
