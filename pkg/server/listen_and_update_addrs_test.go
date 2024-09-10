// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/require"
)

func TestListenAndUpdateAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	addr := "127.0.0.1:0"
	advertiseAddr := "127.0.0.1:0"

	ln, err := ListenAndUpdateAddrs(ctx, &addr, &advertiseAddr, "sql", false)
	require.NoError(t, err)
	require.NotNil(t, ln)
	_, addrPort, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	require.NotZero(t, addrPort)
	_, advertiseAddrPort, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	require.NotZero(t, advertiseAddrPort)
	require.NoError(t, ln.Close())

	proxyLn, err := ListenAndUpdateAddrs(ctx, &addr, &advertiseAddr, "sql", true)
	require.NoError(t, err)
	require.NotNil(t, proxyLn)
	_, ok := proxyLn.(*proxyproto.Listener)
	require.True(t, ok)
	require.NoError(t, proxyLn.Close())
}
