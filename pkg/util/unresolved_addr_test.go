// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnresolvedAddr(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:12345")
	if err != nil {
		t.Fatal(err)
	}
	addr := MakeUnresolvedAddr(tcpAddr.Network(), tcpAddr.String())
	tcpAddr2, err := addr.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if tcpAddr2.Network() != tcpAddr.Network() {
		t.Errorf("networks differ: %s != %s", tcpAddr2.Network(), tcpAddr.Network())
	}
	if tcpAddr2.String() != tcpAddr.String() {
		t.Errorf("strings differ: %s != %s", tcpAddr2.String(), tcpAddr.String())
	}
}

func TestMakeUnresolvedAddrWithDefaults(t *testing.T) {
	const defaultPort = "26257"
	hostname, err := os.Hostname()
	require.NoError(t, err)

	testcases := []struct {
		addr   string
		expect string
	}{
		{"", "HOSTNAME:26257"},
		{":", "HOSTNAME:26257"},
		{":0", "HOSTNAME:0"},
		{":8000", "HOSTNAME:8000"},
		{"localhost", "localhost:26257"},
		{"localhost:8000", "localhost:8000"},
		{"127.0.0.1", "127.0.0.1:26257"},
		{"127.0.0.1:8000", "127.0.0.1:8000"},
		{"::1", "[::1]:26257"},
		{"[::1]", "[::1]:26257"},
		{"::1:8000", "[::1:8000]:26257"},
		{"[::1]:8000", "[::1]:8000"},
		{"f00f:1234::56:1", "[f00f:1234::56:1]:26257"},
		{"👋", "👋:26257"},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.addr, func(t *testing.T) {
			addr := MakeUnresolvedAddrWithDefaults("tcp", tc.addr, defaultPort)
			addr.AddressField = strings.Replace(addr.AddressField, hostname, "HOSTNAME", 1)
			require.Equal(t, tc.expect, addr.String())
		})
	}
}

func TestMakeUnresolvedAddrWithDefaultsNetwork(t *testing.T) {
	testcases := []struct {
		network string
		addr    string
		expect  string
	}{
		{"", "", "tcp"},
		{"", "localhost", "tcp"},
		{"", "::1", "tcp"},
		{"tcp", "::1", "tcp"},
		{"udp", "::1", "udp"},
		{"foo", "::1", "foo"},
	}
	for _, tc := range testcases {
		t.Run(fmt.Sprintf("%s://%s", tc.network, tc.addr), func(t *testing.T) {
			addr := MakeUnresolvedAddrWithDefaults(tc.network, tc.addr, "26257")
			require.Equal(t, tc.expect, addr.Network())
		})
	}
}
