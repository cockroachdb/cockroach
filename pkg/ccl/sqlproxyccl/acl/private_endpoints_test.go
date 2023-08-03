// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl_test

import (
	"context"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/acl"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/pires/go-proxyproto"
	"github.com/pires/go-proxyproto/tlvparse"
	"github.com/stretchr/testify/require"
)

func TestPrivateEndpoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	makeConn := func(endpoint string) acl.ConnectionTags {
		return acl.ConnectionTags{
			IP:         "10.0.0.8",
			TenantID:   roachpb.MustMakeTenantID(42),
			EndpointID: endpoint,
		}
	}

	t.Run("lookup error", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return nil, errors.New("foo")
			},
		}
		err := p.CheckConnection(ctx, makeConn("foo"))
		require.EqualError(t, err, "foo")
	})

	// Public connection should allow, despite not having any private endpoints.
	t.Run("public connection", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					ConnectivityType: tenant.ALLOW_PRIVATE_ONLY,
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn(""))
		require.NoError(t, err)
	})

	// EndpointID does not match.
	t.Run("bad private connection", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					ConnectivityType:        tenant.ALLOW_ALL,
					AllowedPrivateEndpoints: []string{"foo", "baz"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn("bar"))
		require.EqualError(t, err, "connection to '42' denied: cluster does not allow private connections from endpoint 'bar'")
	})

	t.Run("default behavior if no entries", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					ConnectivityType:        tenant.ALLOW_ALL,
					AllowedPrivateEndpoints: []string{},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn("bar"))
		require.EqualError(t, err, "connection to '42' denied: cluster does not allow private connections from endpoint 'bar'")
	})

	t.Run("good private connection", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					ConnectivityType:        tenant.ALLOW_ALL,
					AllowedPrivateEndpoints: []string{"foo"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn("foo"))
		require.NoError(t, err)
	})

	t.Run("disallow private connections", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					ConnectivityType:        tenant.ALLOW_PUBLIC_ONLY,
					AllowedPrivateEndpoints: []string{"foo"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn("foo"))
		require.EqualError(t, err, "connection to '42' denied: cluster does not allow private connections from endpoint 'foo'")
	})
}

func TestFindPrivateEndpointID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	newHeader := func(t *testing.T, tlvs []proxyproto.TLV) *proxyproto.Header {
		t.Helper()
		h := &proxyproto.Header{
			Version:           2,
			Command:           proxyproto.PROXY,
			TransportProtocol: proxyproto.TCPv4,
			SourceAddr: &net.TCPAddr{
				IP:   net.ParseIP("10.1.1.1"),
				Port: 1000,
			},
			DestinationAddr: &net.TCPAddr{
				IP:   net.ParseIP("20.2.2.2"),
				Port: 2000,
			},
		}
		require.NoError(t, h.SetTLVs(tlvs))
		return h
	}
	awsTLV := proxyproto.TLV{
		Type:  tlvparse.PP2_TYPE_AWS,
		Value: []byte{0x01, 0x76, 0x70, 0x63, 0x65, 0x2d, 0x61, 0x62, 0x63, 0x31, 0x32, 0x33},
	}
	azureTLV := proxyproto.TLV{
		Type:  tlvparse.PP2_TYPE_AZURE,
		Value: []byte{0x1, 0xc1, 0x45, 0x0, 0x21},
	}
	gcpTLV := proxyproto.TLV{
		Type:  tlvparse.PP2_TYPE_GCP,
		Value: []byte{'\xff', '\xff', '\xff', '\xff', '\xc0', '\xa8', '\x64', '\x02'},
	}
	miscTLV := proxyproto.TLV{
		Type:  proxyproto.PP2_TYPE_AUTHORITY,
		Value: []byte("cockroachlabs.com"),
	}

	t.Run("not a proxyproto.Conn", func(t *testing.T) {
		p1, _ := net.Pipe()
		defer p1.Close()

		eid, err := acl.FindPrivateEndpointID(p1)
		require.EqualError(t, err, "connection isn't a proxyproto.Conn")
		require.Empty(t, eid)
	})

	for _, tc := range []struct {
		name     string
		tlvs     []proxyproto.TLV
		expected string
	}{
		{"no values", []proxyproto.TLV{}, ""},
		{"unrelated values", []proxyproto.TLV{miscTLV}, ""},
		{"aws values", []proxyproto.TLV{awsTLV}, "vpce-abc123"},
		{"azure values", []proxyproto.TLV{azureTLV}, "553665985"},
		{"gcp values", []proxyproto.TLV{gcpTLV}, "18446744072646845442"},
		{"multiple values", []proxyproto.TLV{gcpTLV, awsTLV, azureTLV}, "vpce-abc123"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p1, p2 := net.Pipe()
			defer p1.Close()
			defer p2.Close()

			go func(tlvs []proxyproto.TLV) {
				_, err := newHeader(t, tlvs).WriteTo(p2)
				require.NoError(t, err)
			}(tc.tlvs)
			conn := proxyproto.NewConn(p1)
			eid, err := acl.FindPrivateEndpointID(conn)
			require.NoError(t, err)
			require.Equal(t, tc.expected, eid)
		})
	}
}
