// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolver

import (
	"context"
	"net"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestParseResolverAddress(t *testing.T) {
	def := ensureHostPort(":", base.DefaultPort)
	testCases := []struct {
		input           string
		success         bool
		resolverType    string
		resolverAddress string
	}{
		// Ports are not checked at parsing time. They are at GetAddress time though.
		{"127.0.0.1:26222", true, "tcp", "127.0.0.1:26222"},
		{":" + base.DefaultPort, true, "tcp", def},
		{"127.0.0.1", true, "tcp", "127.0.0.1:" + base.DefaultPort},
		{"", false, "", ""},
		{"", false, "tcp", ""},
		{":", true, "tcp", def},
	}

	for tcNum, tc := range testCases {
		resolver, err := NewResolver(tc.input)
		if (err == nil) != tc.success {
			t.Errorf("#%d: expected success=%t, got err=%v", tcNum, tc.success, err)
		}
		if err != nil {
			continue
		}
		if resolver.Type() != tc.resolverType {
			t.Errorf("#%d: expected resolverType=%s, got %+v", tcNum, tc.resolverType, resolver)
		}
		if resolver.Addr() != tc.resolverAddress {
			t.Errorf("#%d: expected resolverAddress=%s, got %+v", tcNum, tc.resolverAddress, resolver)
		}
	}
}

func TestGetAddress(t *testing.T) {
	testCases := []struct {
		address      string
		success      bool
		addressType  string
		addressValue string
	}{
		{"127.0.0.1:26222", true, "tcp", "127.0.0.1:26222"},
		{"127.0.0.1", true, "tcp", "127.0.0.1:" + base.DefaultPort},
		{"localhost:80", true, "tcp", "localhost:80"},
	}

	for tcNum, tc := range testCases {
		resolver, err := NewResolver(tc.address)
		if err != nil {
			t.Fatal(err)
		}
		address, err := resolver.GetAddress()
		if (err == nil) != tc.success {
			t.Errorf("#%d: expected success=%t, got err=%v", tcNum, tc.success, err)
		}
		if err != nil {
			continue
		}
		if address.Network() != tc.addressType {
			t.Errorf("#%d: expected address type=%s, got %+v", tcNum, tc.addressType, address)
		}
		if address.String() != tc.addressValue {
			t.Errorf("#%d: expected address value=%s, got %+v", tcNum, tc.addressValue, address)
		}
	}
}

func TestSRV(t *testing.T) {
	type lookupFunc func(service, proto, name string) (string, []*net.SRV, error)

	lookupWithErr := func(err error) lookupFunc {
		return func(service, proto, name string) (string, []*net.SRV, error) {
			if service != "" || proto != "" {
				t.Errorf("unexpected params in erroring LookupSRV() call")
			}
			return "", nil, err
		}
	}

	dnsErr := &net.DNSError{Err: "no such host", Name: "", Server: "", IsTimeout: false}

	lookupSuccess := func(service, proto, name string) (string, []*net.SRV, error) {
		if service != "" || proto != "" {
			t.Errorf("unexpected params in successful LookupSRV() call")
		}

		srvs := []*net.SRV{
			{Target: "node1", Port: 26222},
			{Target: "node2", Port: 35222},
			{Target: "node3", Port: 0},
		}

		return "cluster", srvs, nil
	}

	expectedAddrs := []string{"node1:26222", "node2:35222"}

	testCases := []struct {
		address  string
		lookuper lookupFunc
		want     []string
	}{
		{":26222", nil, nil},
		{"some.host", lookupWithErr(dnsErr), nil},
		{"some.host", lookupWithErr(errors.New("another error")), nil},
		{"some.host", lookupSuccess, expectedAddrs},
		{"some.host:26222", lookupSuccess, expectedAddrs},
		// "real" `lookupSRV` returns "no such host" when resolving IP addresses
		{"127.0.0.1", lookupWithErr(dnsErr), nil},
		{"127.0.0.1:26222", lookupWithErr(dnsErr), nil},
		{"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]", lookupWithErr(dnsErr), nil},
		{"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:26222", lookupWithErr(dnsErr), nil},
	}

	for tcNum, tc := range testCases {
		func() {
			defer TestingOverrideSRVLookupFn(tc.lookuper)()

			resolvers, err := SRV(context.Background(), tc.address)

			if err != nil {
				t.Errorf("#%d: expected success, got err=%v", tcNum, err)
			}

			require.Equal(t, tc.want, resolvers, "Test #%d failed", tcNum)

		}()
	}
}
