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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/acl"
	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPrivateEndpoints(t *testing.T) {
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
		err := p.CheckConnection(ctx, makeConn(""))
		require.EqualError(t, err, "foo")
	})

	// Public connection should allow, despite not having any private endpoints.
	t.Run("public connection", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					ConnectivityType: tenant.ALLOW_PRIVATE,
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
					ConnectivityType: tenant.ALLOW_PUBLIC | tenant.ALLOW_PRIVATE,
					PrivateEndpoints: []string{"foo", "baz"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn("bar"))
		require.EqualError(t, err, "connection to '42' denied: cluster does not allow this private connection")
	})

	t.Run("good private connection", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					ConnectivityType: tenant.ALLOW_PUBLIC | tenant.ALLOW_PRIVATE,
					PrivateEndpoints: []string{"foo"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn("foo"))
		require.NoError(t, err)
	})

	// Does not have tenant.ALLOW_PRIVATE.
	t.Run("disallow private connections", func(t *testing.T) {
		p := &acl.PrivateEndpoints{
			LookupTenantFn: func(ctx context.Context, tenantID roachpb.TenantID) (*tenant.Tenant, error) {
				return &tenant.Tenant{
					ConnectivityType: tenant.ALLOW_PUBLIC,
					PrivateEndpoints: []string{"foo"},
				}, nil
			},
		}
		err := p.CheckConnection(ctx, makeConn("foo"))
		require.EqualError(t, err, "connection to '42' denied: cluster does not allow this private connection")
	})
}
