// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListenAndUpdateAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("don't accept proxy protocol headers", func(t *testing.T) {
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
	})

	t.Run("accept proxy protocol headers", func(t *testing.T) {
		addr := "127.0.0.1:0"
		advertiseAddr := "127.0.0.1:0"
		proxyLn, err := ListenAndUpdateAddrs(ctx, &addr, &advertiseAddr, "sql", true)
		require.NoError(t, err)
		require.NotNil(t, proxyLn)

		proxyLn, ok := proxyLn.(*proxyproto.Listener)
		require.True(t, ok)

		sourceAddr := &net.TCPAddr{
			IP:   net.ParseIP("10.20.30.40").To4(),
			Port: 4242,
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := proxyLn.Accept()
			require.NoError(t, err)

			header := conn.(*proxyproto.Conn).ProxyHeader()
			assert.NotNil(t, header)
			assert.Equal(t, sourceAddr, header.SourceAddr)
		}()

		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer conn.Close()

		header := &proxyproto.Header{
			Version:           2,
			Command:           proxyproto.PROXY,
			TransportProtocol: proxyproto.TCPv4,
			SourceAddr:        sourceAddr,
			DestinationAddr:   conn.RemoteAddr(),
		}
		_, err = header.WriteTo(conn)
		require.NoError(t, err)
		_, err = conn.Write([]byte("ping"))
		require.NoError(t, err)

		wg.Wait()
	})
}
