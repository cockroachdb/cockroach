// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
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
	ctx := context.WithValue(context.Background(), contextKey{}, "v")

	var recv int
	wrappedI := rpc.TestingNewWrappedServerStream(ctx, &ss, func(m interface{}) error {
		if err := ss.RecvMsg(m); err != nil {
			return err
		}
		recv = *(m.(*int))
		return nil
	})
	wrapped := wrappedI.(*rpc.WrappedServerStream)

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

func TestAuthenticateTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	correctOU := []string{security.TenantsOU}
	stid := roachpb.SystemTenantID
	tenTen := roachpb.MustMakeTenantID(10)
	for _, tc := range []struct {
		systemID         roachpb.TenantID
		ous              []string
		commonName       string
		rootDNString     string
		nodeDNString     string
		subjectRequired  bool
		expTenID         roachpb.TenantID
		expErr           string
		tenantScope      uint64
		clientTenantInMD string
		certPrincipalMap string
		certDNSName      string
	}{
		{systemID: stid, ous: correctOU, commonName: "10", expTenID: tenTen},
		{systemID: stid, ous: correctOU, commonName: roachpb.MinTenantID.String(), expTenID: roachpb.MinTenantID},
		{systemID: stid, ous: correctOU, commonName: roachpb.MaxTenantID.String(), expTenID: roachpb.MaxTenantID},
		{systemID: stid, ous: correctOU, commonName: roachpb.SystemTenantID.String() /* "system" */, expErr: `could not parse tenant ID from Common Name \(CN\)`},
		{systemID: stid, ous: correctOU, commonName: "-1", expErr: `could not parse tenant ID from Common Name \(CN\)`},
		{systemID: stid, ous: correctOU, commonName: "0", expErr: `invalid tenant ID 0 in Common Name \(CN\)`},
		{systemID: stid, ous: correctOU, commonName: "1", expErr: `invalid tenant ID 1 in Common Name \(CN\)`},
		{systemID: stid, ous: correctOU, commonName: "root", expErr: `could not parse tenant ID from Common Name \(CN\)`},
		{systemID: stid, ous: correctOU, commonName: "other", expErr: `could not parse tenant ID from Common Name \(CN\)`},
		{systemID: stid, ous: []string{"foo"}, commonName: "other",
			expErr: `need root or node client cert to perform RPCs on this server \(this is tenant system; cert is valid for "other" on all tenants\)`},
		{systemID: stid, ous: nil, commonName: "other",
			expErr: `need root or node client cert to perform RPCs on this server \(this is tenant system; cert is valid for "other" on all tenants\)`},
		{systemID: stid, ous: append([]string{"foo"}, correctOU...), commonName: "other", expErr: `could not parse tenant ID from Common Name`},
		{systemID: stid, ous: nil, commonName: "root"},
		{systemID: stid, ous: nil, commonName: "node"},
		{systemID: stid, ous: nil, commonName: "root", tenantScope: 10,
			expErr: `need root or node client cert to perform RPCs on this server \(this is tenant system; cert is valid for "root" on tenant 10\)`},
		{systemID: tenTen, ous: correctOU, commonName: "10", expTenID: roachpb.TenantID{}},
		{systemID: tenTen, ous: correctOU, commonName: "123", expErr: `client tenant identity \(123\) does not match server`},
		{systemID: tenTen, ous: correctOU, commonName: "1", expErr: `invalid tenant ID 1 in Common Name \(CN\)`},
		{systemID: tenTen, ous: nil, commonName: "root"},
		{systemID: tenTen, ous: nil, commonName: "node"},

		// Passing a client ID in metadata instead of relying only on the TLS cert.
		{clientTenantInMD: "invalid", expErr: `could not parse tenant ID from gRPC metadata`},
		{clientTenantInMD: "1", expErr: `invalid tenant ID 1 in gRPC metadata`},
		{clientTenantInMD: "-1", expErr: `could not parse tenant ID from gRPC metadata`},

		// tenant ID in MD matches that in client cert.
		// Server is KV node: expect tenant authorization.
		{clientTenantInMD: "10",
			systemID: stid, ous: correctOU, commonName: "10", expTenID: tenTen},
		// tenant ID in MD doesn't match that in client cert.
		{clientTenantInMD: "10",
			systemID: stid, ous: correctOU, commonName: "123",
			expErr: `client wants to authenticate as tenant 10, but is using TLS cert for tenant 123`},
		// tenant ID present in MD, but not in client cert. However,
		// client cert is valid. Use MD tenant ID.
		// Server is KV node: expect tenant authorization.
		{clientTenantInMD: "10",
			systemID: stid, ous: nil, commonName: "root", expTenID: tenTen},
		// tenant ID present in MD, but not in client cert. However,
		// client cert is valid. Use MD tenant ID.
		// Server is KV node: expect tenant authorization.
		{clientTenantInMD: "10",
			systemID: stid, ous: nil, commonName: "node", expTenID: tenTen},
		// tenant ID present in MD, but not in client cert. However,
		// client cert is valid. Use MD tenant ID.
		// Server is secondary tenant: do not do additional tenant authorization.
		{clientTenantInMD: "10",
			systemID: tenTen, ous: nil, commonName: "root", expTenID: roachpb.TenantID{}},
		{clientTenantInMD: "10",
			systemID: tenTen, ous: nil, commonName: "node", expTenID: roachpb.TenantID{}},
		// tenant ID present in MD, but not in client cert. Use MD tenant ID.
		// Server tenant ID does not match client tenant ID.
		{clientTenantInMD: "123",
			systemID: tenTen, ous: nil, commonName: "root",
			expErr: `client tenant identity \(123\) does not match server`},
		{systemID: stid, ous: nil, commonName: "foo", rootDNString: "CN=foo"},
		{systemID: stid, ous: nil, commonName: "foo", nodeDNString: "CN=foo"},
		{systemID: stid, ous: nil, commonName: "foo", rootDNString: "CN=bar",
			expErr: `need root or node client cert to perform RPCs on this server. cert dn did not match set root or node dn`},
		{systemID: stid, ous: nil, commonName: "foo", nodeDNString: "CN=bar",
			expErr: `need root or node client cert to perform RPCs on this server. cert dn did not match set root or node dn`},
		{systemID: stid, ous: nil, commonName: "foo", certPrincipalMap: "foo:root"},
		{systemID: stid, ous: nil, commonName: "foo", certPrincipalMap: "foo:node"},
		{systemID: stid, ous: nil, commonName: "foo", certPrincipalMap: "foo:bar",
			expErr: `need root or node client cert to perform RPCs on this server \(this is tenant system; cert is valid for "bar" on all tenants\)`},
		{systemID: stid, ous: nil, commonName: "foo", certDNSName: "root"},
		{systemID: stid, ous: nil, commonName: "foo", certDNSName: "node"},
		{systemID: stid, ous: nil, commonName: "foo", certDNSName: "bar",
			expErr: `need root or node client cert to perform RPCs on this server \(this is tenant system; cert is valid for "foo" on all tenants, "bar" on all tenants\)`},
		{systemID: stid, ous: nil, commonName: "foo", certDNSName: "bar", certPrincipalMap: "bar:root"},
		{systemID: stid, ous: nil, commonName: "foo", certDNSName: "bar", certPrincipalMap: "bar:node"},
		{systemID: stid, ous: nil, commonName: "foo", subjectRequired: true,
			expErr: `root and node roles do not have valid DNs set which subject_required cluster setting mandates`},
		{systemID: stid, ous: nil, commonName: "foo", subjectRequired: true, rootDNString: "CN=bar",
			expErr: `need root or node client cert to perform RPCs on this server: cert dn did not match set root or node dn`},
		{systemID: stid, ous: nil, commonName: "foo", subjectRequired: true, nodeDNString: "CN=bar",
			expErr: `need root or node client cert to perform RPCs on this server: cert dn did not match set root or node dn`},
		{systemID: stid, ous: nil, commonName: "foo", subjectRequired: true, rootDNString: "CN=foo"},
		{systemID: stid, ous: nil, commonName: "foo", subjectRequired: true, nodeDNString: "CN=foo"},
		{systemID: stid, ous: nil, commonName: "foo", subjectRequired: true,
			rootDNString: "CN=foo", nodeDNString: "CN=bar"},
	} {
		t.Run(fmt.Sprintf("from %v to %v (md %q)", tc.commonName, tc.systemID, tc.clientTenantInMD), func(t *testing.T) {
			err := security.SetCertPrincipalMap(strings.Split(tc.certPrincipalMap, ","))
			if err != nil {
				t.Fatal(err)
			}
			if tc.rootDNString != "" {
				err = security.SetRootSubject(tc.rootDNString)
				if err != nil {
					t.Fatalf("could not set root subject DN, err: %v", err)
				}
			}
			if tc.nodeDNString != "" {
				err = security.SetNodeSubject(tc.nodeDNString)
				if err != nil {
					t.Fatalf("could not set node subject DN, err: %v", err)
				}
			}
			defer func() {
				security.UnsetRootSubject()
				security.UnsetNodeSubject()
			}()

			cert := &x509.Certificate{
				Subject: pkix.Name{
					CommonName:         tc.commonName,
					OrganizationalUnit: tc.ous,
				},
			}
			cert.RawSubject, err = asn1.Marshal(cert.Subject.ToRDNSequence())
			if err != nil {
				t.Fatalf("unable to marshal rdn sequence to raw subject, err: %v", err)
			}
			if tc.certDNSName != "" {
				cert.DNSNames = append(cert.DNSNames, tc.certDNSName)
			}
			if tc.tenantScope > 0 {
				tenantSANs, err := security.MakeTenantURISANs(
					username.MakeSQLUsernameFromPreNormalizedString(tc.commonName),
					[]roachpb.TenantID{roachpb.MustMakeTenantID(tc.tenantScope)})
				require.NoError(t, err)
				cert.URIs = append(cert.URIs, tenantSANs...)
			}
			tlsInfo := credentials.TLSInfo{
				State: tls.ConnectionState{
					PeerCertificates: []*x509.Certificate{cert},
				},
			}
			p := peer.Peer{AuthInfo: tlsInfo}
			ctx := peer.NewContext(context.Background(), &p)

			if tc.clientTenantInMD != "" {
				md := metadata.MD{"client-tid": []string{tc.clientTenantInMD}}
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			sv := &settings.Values{}
			sv.Init(ctx, settings.TestOpaque)
			u := settings.NewUpdater(sv)
			err = u.Set(ctx, security.ClientCertSubjectRequiredSettingName,
				settings.EncodedValue{Value: strconv.FormatBool(tc.subjectRequired), Type: "b"})
			require.NoError(t, err)

			tenID, err := rpc.TestingAuthenticateTenant(ctx, tc.systemID, sv)

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

func BenchmarkAuthenticate(b *testing.B) {
	correctOU := []string{security.TenantsOU}
	stid := roachpb.SystemTenantID
	for _, tc := range []struct {
		name         string
		systemID     roachpb.TenantID
		ous          []string
		commonName   string
		rootDNString string
		nodeDNString string
	}{
		// Success case with a tenant certificate.
		{name: "tenTen", systemID: stid, ous: correctOU, commonName: "10"},
		// Success cases with root or node DN.
		{name: "rootDN", systemID: stid, ous: nil, commonName: "foo", rootDNString: "CN=foo"},
		{name: "nodeDN", systemID: stid, ous: nil, commonName: "foo", nodeDNString: "CN=foo"},
		// Success cases that fallback to the global scope.
		{name: "commonRoot", systemID: stid, ous: nil, commonName: "root"},
		{name: "commonNode", systemID: stid, ous: nil, commonName: "node"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			var err error
			if tc.rootDNString != "" {
				err = security.SetRootSubject(tc.rootDNString)
				if err != nil {
					b.Fatalf("could not set root subject DN, err: %v", err)
				}
			}
			if tc.nodeDNString != "" {
				err = security.SetNodeSubject(tc.nodeDNString)
				if err != nil {
					b.Fatalf("could not set node subject DN, err: %v", err)
				}
			}
			defer func() {
				security.UnsetRootSubject()
				security.UnsetNodeSubject()
			}()

			cert := &x509.Certificate{
				Subject: pkix.Name{
					CommonName:         tc.commonName,
					OrganizationalUnit: tc.ous,
				},
			}
			cert.RawSubject, err = asn1.Marshal(cert.Subject.ToRDNSequence())
			if err != nil {
				b.Fatalf("unable to marshal rdn sequence to raw subject, err: %v", err)
			}
			tlsInfo := credentials.TLSInfo{
				State: tls.ConnectionState{
					PeerCertificates: []*x509.Certificate{cert},
				},
			}
			p := peer.Peer{AuthInfo: tlsInfo}
			ctx := peer.NewContext(context.Background(), &p)
			sv := &settings.Values{}
			sv.Init(ctx, settings.TestOpaque)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := rpc.TestingAuthenticateTenant(ctx, tc.systemID, sv)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func prefix(tenID uint64, key string) string {
	tenPrefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(tenID))
	return string(append(tenPrefix, []byte(key)...))
}

func makeSpanShared(t *testing.T, key string, endKey ...string) roachpb.Span {
	s := roachpb.Span{Key: roachpb.Key(key)}
	if len(endKey) > 1 {
		t.Fatalf("unexpected endKey vararg %v", endKey)
	} else if len(endKey) == 1 {
		s.EndKey = roachpb.Key(endKey[0])
	}
	return s
}

func makeReqShared(t *testing.T, key string, endKey ...string) kvpb.Request {
	s := makeSpanShared(t, key, endKey...)
	h := kvpb.RequestHeaderFromSpan(s)
	return &kvpb.ScanRequest{RequestHeader: h}
}

func makeReqs(reqs ...kvpb.Request) []kvpb.RequestUnion {
	ru := make([]kvpb.RequestUnion, len(reqs))
	for i, r := range reqs {
		ru[i].MustSetInner(r)
	}
	return ru
}

func TestTenantAuthRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tenID := roachpb.MustMakeTenantID(10)
	makeSpan := func(key string, endKey ...string) roachpb.Span {
		return makeSpanShared(t, key, endKey...)
	}
	makeReq := func(key string, endKey ...string) kvpb.Request {
		return makeReqShared(t, key, endKey...)
	}
	makeAdminSplitReq := func(key string) kvpb.Request {
		s := makeSpan(key)
		h := kvpb.RequestHeaderFromSpan(s)
		return &kvpb.AdminSplitRequest{RequestHeader: h, SplitKey: s.Key}
	}
	makeAdminScatterReq := func(key string) kvpb.Request {
		s := makeSpan(key)
		h := kvpb.RequestHeaderFromSpan(s)
		return &kvpb.AdminScatterRequest{RequestHeader: h}
	}
	makeSystemSpanConfigTarget := func(source, target uint64) roachpb.SpanConfigTarget {
		return roachpb.SpanConfigTarget{
			Union: &roachpb.SpanConfigTarget_SystemSpanConfigTarget{
				SystemSpanConfigTarget: &roachpb.SystemSpanConfigTarget{
					SourceTenantID: roachpb.MustMakeTenantID(source),
					Type:           roachpb.NewSpecificTenantKeyspaceTargetType(roachpb.MustMakeTenantID(target)),
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
	makeSpanConfigConformanceReq := func(
		span roachpb.Span,
	) *roachpb.SpanConfigConformanceRequest {
		return &roachpb.SpanConfigConformanceRequest{Spans: []roachpb.Span{span}}
	}

	makeGetRangeDescriptorsReq := func(span roachpb.Span) *kvpb.GetRangeDescriptorsRequest {
		return &kvpb.GetRangeDescriptorsRequest{
			Span: span,
		}
	}

	tenantTwo := roachpb.MustMakeTenantID(2)
	makeTimeseriesQueryReq := func(tenantID *roachpb.TenantID) *tspb.TimeSeriesQueryRequest {
		req := &tspb.TimeSeriesQueryRequest{
			Queries: []tspb.Query{{}},
		}
		if tenantID != nil {
			req.Queries[0].TenantID = *tenantID
		}
		return req
	}

	const noError = ""
	for method, tests := range map[string][]struct {
		req    interface{}
		expErr string
	}{
		"/cockroach.roachpb.Internal/Batch": {
			{
				req:    &kvpb.BatchRequest{},
				expErr: `requested key span /Max not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq("a", "b"),
				)},
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(5, "a"), prefix(5, "b")),
				)},
				expErr: `requested key span /Tenant/5{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: noError,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(50, "a"), prefix(50, "b")),
				)},
				expErr: `requested key span /Tenant/50{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq("a", "b"),
					makeReq(prefix(5, "a"), prefix(5, "b")),
				)},
				expErr: `requested key span {a-/Tenant/5b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(5, "a"), prefix(5, "b")),
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: `requested key span /Tenant/{5a-10b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq("a", prefix(10, "b")),
				)},
				expErr: `requested key span {a-/Tenant/10b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(20, "b")),
				)},
				expErr: `requested key span /Tenant/{10a-20b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeAdminSplitReq("a"),
				)},
				expErr: `requested key span a{-\\x00} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeAdminSplitReq(prefix(10, "a")),
				)},
				expErr: noError,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeAdminSplitReq(prefix(50, "a")),
				)},
				expErr: `requested key span /Tenant/50a{-\\x00} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeAdminSplitReq(prefix(10, "a")),
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: noError,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
					makeAdminSplitReq(prefix(10, "a")),
				)},
				expErr: noError,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeAdminScatterReq("a"),
				)},
				expErr: `requested key span a{-\\x00} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeAdminScatterReq(prefix(10, "a")),
				)},
				expErr: noError,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeAdminScatterReq(prefix(50, "a")),
				)},
				expErr: `requested key span /Tenant/50a{-\\x00} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeAdminScatterReq(prefix(10, "a")),
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: noError,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
					makeAdminScatterReq(prefix(10, "a")),
				)},
				expErr: noError,
			},
		},
		"/cockroach.roachpb.Internal/BatchStream": {
			{
				req:    &kvpb.BatchRequest{},
				expErr: `requested key span /Max not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq("a", "b"),
				)},
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(5, "a"), prefix(5, "b")),
				)},
				expErr: `requested key span /Tenant/5{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReq(prefix(10, "a"), prefix(10, "b")),
				)},
				expErr: noError,
			},
		},
		"/cockroach.roachpb.Internal/RangeLookup": {
			{
				req:    &kvpb.RangeLookupRequest{},
				expErr: `requested key /Min not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &kvpb.RangeLookupRequest{Key: roachpb.RKey("a")},
				expErr: `requested key "a" not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &kvpb.RangeLookupRequest{Key: roachpb.RKey(prefix(5, "a"))},
				expErr: `requested key /Tenant/5"a" not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &kvpb.RangeLookupRequest{Key: roachpb.RKey(prefix(10, "a"))},
				expErr: noError,
			},
			{
				req:    &kvpb.RangeLookupRequest{Key: roachpb.RKey(prefix(50, "a"))},
				expErr: `requested key /Tenant/50"a" not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
		},
		"/cockroach.roachpb.Internal/RangeFeed": {
			{
				req:    &kvpb.RangeFeedRequest{},
				expErr: `requested key span /Min not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &kvpb.RangeFeedRequest{Span: makeSpan("a", "b")},
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &kvpb.RangeFeedRequest{Span: makeSpan(prefix(5, "a"), prefix(5, "b"))},
				expErr: `requested key span /Tenant/5{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &kvpb.RangeFeedRequest{Span: makeSpan(prefix(10, "a"), prefix(10, "b"))},
				expErr: noError,
			},
			{
				req:    &kvpb.RangeFeedRequest{Span: makeSpan(prefix(50, "a"), prefix(50, "b"))},
				expErr: `requested key span /Tenant/50{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &kvpb.RangeFeedRequest{Span: makeSpan("a", prefix(10, "b"))},
				expErr: `requested key span {a-/Tenant/10b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    &kvpb.RangeFeedRequest{Span: makeSpan(prefix(10, "a"), prefix(20, "b"))},
				expErr: `requested key span /Tenant/{10a-20b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
		},
		"/cockroach.roachpb.Internal/GossipSubscription": {
			{
				req:    &kvpb.GossipSubscriptionRequest{},
				expErr: noError,
			},
			{
				req:    &kvpb.GossipSubscriptionRequest{Patterns: []string{"node:.*"}},
				expErr: noError,
			},
			{
				req:    &kvpb.GossipSubscriptionRequest{Patterns: []string{"system-db"}},
				expErr: noError,
			},
			{
				req:    &kvpb.GossipSubscriptionRequest{Patterns: []string{"table-stat-added"}},
				expErr: `requested pattern "table-stat-added" not permitted`,
			},
			{
				req:    &kvpb.GossipSubscriptionRequest{Patterns: []string{"node:.*", "system-db"}},
				expErr: noError,
			},
			{
				req:    &kvpb.GossipSubscriptionRequest{Patterns: []string{"node:.*", "system-db", "table-stat-added"}},
				expErr: `requested pattern "table-stat-added" not permitted`,
			},
		},
		"/cockroach.roachpb.Internal/TokenBucket": {
			{
				req:    &kvpb.TokenBucketRequest{TenantID: tenID.ToUint64()},
				expErr: noError,
			},
			{
				req:    &kvpb.TokenBucketRequest{TenantID: roachpb.SystemTenantID.ToUint64()},
				expErr: `token bucket request for tenant system not permitted`,
			},
			{
				req:    &kvpb.TokenBucketRequest{TenantID: 13},
				expErr: `token bucket request for tenant 13 not permitted`,
			},
			{
				req:    &kvpb.TokenBucketRequest{},
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
				expErr: `requested key span /Tenant/5{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
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
				expErr: `requested key span /Tenant/50{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeGetSpanConfigsReq(
					makeSpanTarget(makeSpan("a", prefix(10, "b"))),
				),
				expErr: `requested key span {a-/Tenant/10b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeGetSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(20, "b"))),
				),
				expErr: `requested key span /Tenant/{10a-20b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
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
							SourceTenantID: roachpb.MustMakeTenantID(10),
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
							SourceTenantID: roachpb.MustMakeTenantID(20),
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
				expErr: `requested key span /Tenant/5{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
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
				expErr: `requested key span /Tenant/50{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan("a", prefix(10, "b"))),
					true,
				),
				expErr: `requested key span {a-/Tenant/10b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(20, "b"))),
					true,
				),
				expErr: `requested key span /Tenant/{10a-20b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
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
				expErr: `requested key span /Tenant/5{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
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
				expErr: `requested key span /Tenant/50{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan("a", prefix(10, "b"))),
					false,
				),
				expErr: `requested key span {a-/Tenant/10b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req: makeUpdateSpanConfigsReq(
					makeSpanTarget(makeSpan(prefix(10, "a"), prefix(20, "b"))),
					false,
				),
				expErr: `requested key span /Tenant/{10a-20b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
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
							SourceTenantID: roachpb.MustMakeTenantID(10),
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
							SourceTenantID: roachpb.MustMakeTenantID(10),
							Type:           roachpb.NewEntireKeyspaceTargetType(),
						},
					},
				}, true),
				expErr: `secondary tenants cannot target the entire keyspace`,
			},
		},
		"/cockroach.roachpb.Internal/SpanConfigConformance": {
			{
				req:    &roachpb.SpanConfigConformanceRequest{},
				expErr: noError,
			},
			{
				req:    makeSpanConfigConformanceReq(makeSpan("a", "b")),
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeSpanConfigConformanceReq(makeSpan(prefix(5, "a"), prefix(5, "b"))),
				expErr: `requested key span /Tenant/5{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeSpanConfigConformanceReq(makeSpan(prefix(10, "a"), prefix(10, "b"))),
				expErr: noError,
			},
			{
				req:    makeSpanConfigConformanceReq(makeSpan(prefix(50, "a"), prefix(50, "b"))),
				expErr: `requested key span /Tenant/50{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeSpanConfigConformanceReq(makeSpan("a", prefix(10, "b"))),
				expErr: `requested key span {a-/Tenant/10b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeSpanConfigConformanceReq(makeSpan(prefix(10, "a"), prefix(20, "b"))),
				expErr: `requested key span /Tenant/{10a-20b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
		},
		"/cockroach.roachpb.Internal/GetAllSystemSpanConfigsThatApply": {
			{
				req:    &roachpb.GetAllSystemSpanConfigsThatApplyRequest{},
				expErr: "GetAllSystemSpanConfigsThatApply request with unspecified tenant not permitted",
			},
			{
				req: &roachpb.GetAllSystemSpanConfigsThatApplyRequest{
					TenantID: roachpb.MustMakeTenantID(20),
				},
				expErr: "GetAllSystemSpanConfigsThatApply request for tenant 20 not permitted",
			},
			{
				req: &roachpb.GetAllSystemSpanConfigsThatApplyRequest{
					TenantID: roachpb.SystemTenantID,
				},
				expErr: "GetAllSystemSpanConfigsThatApply request for tenant system not permitted",
			},
			{
				req: &roachpb.GetAllSystemSpanConfigsThatApplyRequest{
					TenantID: roachpb.MustMakeTenantID(10),
				},
				expErr: noError,
			},
		},
		"/cockroach.roachpb.Internal/GetRangeDescriptors": {
			{
				req:    makeGetRangeDescriptorsReq(makeSpan("a", "b")),
				expErr: `requested key span {a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeGetRangeDescriptorsReq(makeSpan(prefix(5, "a"), prefix(5, "b"))),
				expErr: `requested key span /Tenant/5{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeGetRangeDescriptorsReq(makeSpan(prefix(10, "a"), prefix(10, "b"))),
				expErr: noError,
			},
			{
				req:    makeGetRangeDescriptorsReq(makeSpan(prefix(50, "a"), prefix(50, "b"))),
				expErr: `requested key span /Tenant/50{a-b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeGetRangeDescriptorsReq(makeSpan("a", prefix(10, "b"))),
				expErr: `requested key span {a-/Tenant/10b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
			{
				req:    makeGetRangeDescriptorsReq(makeSpan(prefix(10, "a"), prefix(20, "b"))),
				expErr: `requested key span /Tenant/{10a-20b} not fully contained in tenant keyspace /Tenant/1{0-1}`,
			},
		},
		"/cockroach.ts.tspb.TimeSeries/Query": {
			{
				req:    makeTimeseriesQueryReq(&tenID),
				expErr: noError,
			},
			{
				req:    makeTimeseriesQueryReq(nil),
				expErr: noError,
			},
			{
				req:    makeTimeseriesQueryReq(&roachpb.TenantID{}),
				expErr: noError,
			},
			{
				req:    makeTimeseriesQueryReq(&tenantTwo),
				expErr: `tsdb query with invalid tenant not permitted`,
			},
		},

		"/cockroach.rpc.Heartbeat/Ping": {
			{req: &rpc.PingRequest{}, expErr: noError},
		},
		"/cockroach.rpc.Testing/Foo": {
			{req: "req", expErr: `unknown method "/cockroach.rpc.Testing/Foo"`},
		},
	} {
		t.Run(strings.ReplaceAll(method, "/", "_"), func(t *testing.T) {
			ctx := context.Background()
			for _, tc := range tests {
				t.Run("", func(t *testing.T) {
					testutils.RunTrueAndFalse(t, "cross", func(t *testing.T, canCrossRead bool) {
						err := rpc.TestingAuthorizeTenantRequest(ctx, &settings.Values{}, tenID, method, tc.req, mockAuthorizer{
							hasCrossTenantRead:                 canCrossRead,
							hasCapabilityForBatch:              true,
							hasNodestatusCapability:            true,
							hasTSDBQueryCapability:             true,
							hasNodelocalStorageCapability:      true,
							hasExemptFromRateLimiterCapability: true,
							hasTSDBAllCapability:               true,
						})

						// If the "expected" error is about tenant bounds but the tenant has
						// cross-read capability and the request is a read, expect no error.
						if canCrossRead && strings.Contains(tc.expErr, "fully contained") {
							switch method {
							case "/cockroach.roachpb.Internal/Batch", "/cockroach.roachpb.Internal/BatchStream":
								if tc.req.(*kvpb.BatchRequest).IsReadOnly() {
									tc.expErr = noError
								}
							case "/cockroach.roachpb.Internal/GetRangeDescriptors":
								tc.expErr = noError
							case "/cockroach.roachpb.Internal/RangeLookup":
								tc.expErr = noError
							case "/cockroach.roachpb.Internal/GetSpanConfigs":
								tc.expErr = noError
							}
						}

						if tc.expErr == noError {
							require.NoError(t, err)
						} else {
							require.Error(t, err)
							require.Equal(t, codes.Unauthenticated, status.Code(err))
							require.Regexp(t, tc.expErr, err)
						}
					})
				})
			}
		})
	}
}

// TestSpecialTenantID ensures that tenant ID with special encodings
// are handled properly by authz code.
func TestSpecialTenantID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// specialTenantID can be set to any integer value such that the KV
	// encoding of the PrefixEnd of the value, is different from the encoding
	// of the value + 1.
	// We know this is true of at least 109 and 511.
	const specialTenantIDVal = 511

	// First verify the special property.
	keyStart := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(specialTenantIDVal))
	keyEnd := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(specialTenantIDVal + 1))
	require.NotEqual(t, keyEnd, keyStart.PrefixEnd()) // these two would otherwise be equal for non-special tenant IDs.

	// Now check that all the virtual keyspace is properly authorized
	// by the authz code for various special tenant IDs.
	for _, tenIDval := range []uint64{1, 10, specialTenantIDVal, roachpb.MaxTenantID.ToUint64()} {
		t.Run(fmt.Sprint(tenIDval), func(t *testing.T) {
			tenID := roachpb.MustMakeTenantID(tenIDval)
			tenantKeyspace := keys.MakeTenantSpan(tenID)

			req := &kvpb.BatchRequest{Requests: makeReqs(
				&kvpb.ScanRequest{RequestHeader: kvpb.RequestHeaderFromSpan(tenantKeyspace)})}
			ctx := context.Background()
			err := rpc.TestingAuthorizeTenantRequest(
				ctx, &settings.Values{},
				tenID, "/cockroach.roachpb.Internal/Batch", req,
				tenantcapabilitiesauthorizer.NewAllowEverythingAuthorizer(),
			)
			require.NoError(t, err)
		})
	}
}

// TestTenantAuthCapabilityChecks ensures capability checks are performed
// correctly by the tenant authorizer.
func TestTenantAuthCapabilityChecks(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeTimeseriesQueryReq := func(tenantID *roachpb.TenantID) *tspb.TimeSeriesQueryRequest {
		req := &tspb.TimeSeriesQueryRequest{
			Queries: []tspb.Query{{}},
		}
		if tenantID != nil {
			req.Queries[0].TenantID = *tenantID
		}
		return req
	}

	tenID := roachpb.MustMakeTenantID(10)
	for method, tests := range map[string][]struct {
		req                 interface{}
		configureAuthorizer func(authorizer *mockAuthorizer)
		expErr              string
	}{
		"/cockroach.roachpb.Internal/Batch": {
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReqShared(t, prefix(10, "a"), prefix(10, "b")),
				)},
				configureAuthorizer: func(authorizer *mockAuthorizer) {
					authorizer.hasCapabilityForBatch = true
				},
				expErr: "",
			},
			{
				req: &kvpb.BatchRequest{Requests: makeReqs(
					makeReqShared(t, prefix(10, "a"), prefix(10, "b")),
				)},
				configureAuthorizer: func(authorizer *mockAuthorizer) {
					authorizer.hasCapabilityForBatch = false
				},
				expErr: "tenant does not have capability",
			},
		},
		"/cockroach.ts.tspb.TimeSeries/Query": {
			{
				req: makeTimeseriesQueryReq(&tenID),
				configureAuthorizer: func(authorizer *mockAuthorizer) {
					authorizer.hasTSDBQueryCapability = true
				},
				expErr: "",
			},
			{
				req: makeTimeseriesQueryReq(&tenID),
				configureAuthorizer: func(authorizer *mockAuthorizer) {
					authorizer.hasTSDBQueryCapability = false
				},
				expErr: "tenant does not have capability",
			},
			{
				req: makeTimeseriesQueryReq(nil),
				configureAuthorizer: func(authorizer *mockAuthorizer) {
					authorizer.hasTSDBQueryCapability = false
					authorizer.hasTSDBAllCapability = true
				},
				expErr: "tenant does not have capability",
			},
			{
				req: makeTimeseriesQueryReq(nil),
				configureAuthorizer: func(authorizer *mockAuthorizer) {
					authorizer.hasTSDBQueryCapability = true
					authorizer.hasTSDBAllCapability = true
				},
				expErr: "",
			},
		},
	} {
		ctx := context.Background()
		for _, tc := range tests {
			authorizer := mockAuthorizer{}
			tc.configureAuthorizer(&authorizer)
			err := rpc.TestingAuthorizeTenantRequest(
				ctx, &settings.Values{}, tenID, method, tc.req, authorizer,
			)
			if tc.expErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, codes.Unauthenticated, status.Code(err))
				require.Regexp(t, tc.expErr, err)
			}
		}
	}
}

type mockAuthorizer struct {
	hasCrossTenantRead                 bool
	hasCapabilityForBatch              bool
	hasNodestatusCapability            bool
	hasTSDBQueryCapability             bool
	hasNodelocalStorageCapability      bool
	hasExemptFromRateLimiterCapability bool
	hasTSDBAllCapability               bool
}

func (m mockAuthorizer) HasTSDBAllMetricsCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	if m.hasTSDBAllCapability {
		return nil
	}
	return errors.New("tenant does not have capability")
}

func (m mockAuthorizer) HasProcessDebugCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	return errors.New("tenant does not have capability")
}

func (m mockAuthorizer) HasCrossTenantRead(ctx context.Context, tenID roachpb.TenantID) bool {
	return m.hasCrossTenantRead
}

var _ tenantcapabilities.Authorizer = &mockAuthorizer{}

// HasCapabilityForBatch implements the tenantcapabilities.Authorizer interface.
func (m mockAuthorizer) HasCapabilityForBatch(
	context.Context, roachpb.TenantID, *kvpb.BatchRequest,
) error {
	if m.hasCapabilityForBatch {
		return nil
	}
	return errors.New("tenant does not have capability")
}

// BindReader implements the tenantcapabilities.Authorizer interface.
func (m mockAuthorizer) BindReader(tenantcapabilities.Reader) {
	panic("unimplemented")
}

func (m mockAuthorizer) HasNodeStatusCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if m.hasNodestatusCapability {
		return nil
	}
	return errors.New("tenant does not have capability")
}

func (m mockAuthorizer) HasTSDBQueryCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if m.hasTSDBQueryCapability {
		return nil
	}
	return errors.New("tenant does not have capability")
}

func (m mockAuthorizer) HasNodelocalStorageCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	if m.hasNodelocalStorageCapability {
		return nil
	}
	return errors.New("tenant does not have capability")
}

func (m mockAuthorizer) IsExemptFromRateLimiting(context.Context, roachpb.TenantID) bool {
	return m.hasExemptFromRateLimiterCapability
}

type contextKey struct{}
