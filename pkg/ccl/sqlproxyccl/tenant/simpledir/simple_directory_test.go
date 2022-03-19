// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package simpledir_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant/simpledir"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSimpleDirectory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tenantID := roachpb.MakeTenantID(10)
	d := simpledir.NewSimpleDirectory("localhost:42")

	t.Run("resolve_error", func(t *testing.T) {
		defer testutils.TestingHook(
			&simpledir.ResolveTCPAddr,
			func(network, addr string) (*net.TCPAddr, error) {
				return nil, errors.New("invalid address")
			},
		)()

		require.NoError(t, d.ReportFailure(ctx, tenantID, "localhost:42"))

		addr, err := d.EnsureTenantAddr(ctx, tenantID, "foo")
		require.Error(t, err)
		s := status.Convert(err)
		require.Equal(t, codes.NotFound, s.Code())
		require.Equal(t, "SQL pod address cannot be resolved: invalid address", s.Message())
		require.Equal(t, "", addr)

		addr, err = d.EnsureTenantAddr(ctx, tenantID, "bar")
		s = status.Convert(err)
		require.Equal(t, codes.NotFound, s.Code())
		require.Equal(t, "SQL pod address cannot be resolved: invalid address", s.Message())
		require.Equal(t, "", addr)

		var addrs []string
		addrs, err = d.LookupTenantAddrs(ctx, tenantID)
		s = status.Convert(err)
		require.Equal(t, codes.NotFound, s.Code())
		require.Equal(t, "SQL pod address cannot be resolved: invalid address", s.Message())
		require.Nil(t, addrs)
	})

	t.Run("successful", func(t *testing.T) {
		resolveTCPAddrCount := 0
		defer testutils.TestingHook(
			&simpledir.ResolveTCPAddr,
			func(network, addr string) (*net.TCPAddr, error) {
				resolveTCPAddrCount++
				return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 26257}, nil
			},
		)()

		require.NoError(t, d.ReportFailure(ctx, tenantID, "localhost:42"))

		addr, err := d.EnsureTenantAddr(ctx, tenantID, "foo")
		require.NoError(t, err)
		require.Equal(t, "localhost:42", addr)
		require.Equal(t, 1, resolveTCPAddrCount)

		addr, err = d.EnsureTenantAddr(ctx, tenantID, "bar")
		require.NoError(t, err)
		require.Equal(t, "localhost:42", addr)
		require.Equal(t, 2, resolveTCPAddrCount)

		var addrs []string
		addrs, err = d.LookupTenantAddrs(ctx, tenantID)
		require.NoError(t, err)
		require.Equal(t, []string{"localhost:42"}, addrs)
		require.Equal(t, 3, resolveTCPAddrCount)
	})
}
