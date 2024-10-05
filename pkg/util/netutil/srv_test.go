// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package netutil

import (
	"context"
	"net"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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
