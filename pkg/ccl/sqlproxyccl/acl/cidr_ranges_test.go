// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/acl"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCIDRRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)
	ctx := context.Background()

	tenantID := roachpb.MustMakeTenantID(42)
	makeConn := func(endpoint string) acl.ConnectionTags {
		return acl.ConnectionTags{
			IP:         "127.0.0.1",
			TenantID:   tenantID,
			EndpointID: endpoint,
		}
	}

	t.Run("lookup error", func(t *testing.T) {
		p := &acl.CIDRRanges{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return nil, errors.New("foo")
			},
		}
		err := p.CheckConnection(ctx, makeConn(""))
		require.EqualError(t, err, "foo")
	})

	// Private connection should be allowed, despite not having any CIDR ranges.
	t.Run("private connection", func(t *testing.T) {
		p := &acl.CIDRRanges{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn("foo"))
		require.NoError(t, err)
	})

	// CIDR ranges do not match.
	t.Run("bad public connection", func(t *testing.T) {
		p := &acl.CIDRRanges{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					AllowedCIDRRanges: []string{"127.0.0.0/32", "10.0.0.8/16"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn(""))
		require.EqualError(t, err, "cluster does not allow public connections from IP 127.0.0.1")
	})

	t.Run("default behavior if no entries", func(t *testing.T) {
		p := &acl.CIDRRanges{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					AllowedCIDRRanges: []string{},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn(""))
		require.EqualError(t, err, "cluster does not allow public connections from IP 127.0.0.1")
	})

	t.Run("good public connection", func(t *testing.T) {
		p := &acl.CIDRRanges{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					AllowedCIDRRanges: []string{"0.0.0.0/0"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn(""))
		require.NoError(t, err)
	})

	t.Run("could not parse connection IP", func(t *testing.T) {
		p := &acl.CIDRRanges{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					AllowedCIDRRanges: []string{"127.0.0.1/32"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, acl.ConnectionTags{
			IP:       "invalid-value",
			TenantID: tenantID,
		})
		require.EqualError(t, err, "could not parse IP address: 'invalid-value'")
	})

	t.Run("could not parse CIDR range", func(t *testing.T) {
		p := &acl.CIDRRanges{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					AllowedCIDRRanges: []string{"127.0.0.1"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn(""))
		require.EqualError(t, err, "invalid CIDR address: 127.0.0.1")
	})
}
