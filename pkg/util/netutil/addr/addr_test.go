// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package addr_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/stretchr/testify/require"
)

func TestPortRangeSetter(t *testing.T) {
	testData := []struct {
		v      string
		lower  int
		upper  int
		str    string
		errStr string
	}{
		{"5-10", 5, 10, "5-10", ""},
		{"5-", 5, 0, "", "too few parts"},
		{"5", 5, 0, "", "too few parts"},
		{"5-8-10", 0, 0, "", "too many parts"},
		{"a-5", 0, 0, "", "invalid syntax"},
		{"5-b", 0, 0, "", "invalid syntax"},
		{"10-5", 0, 0, "", "lower bound (10) > upper bound (5)"},
	}
	for _, tc := range testData {
		t.Run(tc.v, func(t *testing.T) {
			var upper, lower int
			s := addr.NewPortRangeSetter(&lower, &upper)
			err := s.Set(tc.v)
			if tc.errStr != "" {
				require.ErrorContains(t, err, tc.errStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.str, s.String())
				require.Equal(t, tc.upper, upper)
				require.Equal(t, tc.lower, lower)
			}
		})
	}
}

func TestSplitHostPort(t *testing.T) {
	testData := []struct {
		v           string
		defaultPort string
		addr        string
		port        string
		err         string
	}{
		{"127.0.0.1", "", "127.0.0.1", "", ""},
		{"127.0.0.1:123", "", "127.0.0.1", "123", ""},
		{"127.0.0.1", "123", "127.0.0.1", "123", ""},
		{"127.0.0.1:456", "123", "127.0.0.1", "456", ""},
		{"[::1]", "", "::1", "", ""},
		{"[::1]:123", "", "::1", "123", ""},
		{"[::1]", "123", "::1", "123", ""},
		{"[::1]:456", "123", "::1", "456", ""},
		{"::1", "", "", "", "invalid address format: \"::1\""},
		{"[123", "", "", "", `address \[123:: missing ']' in address`},
		{":", "123", "", "123", ""},
	}

	for _, test := range testData {
		t.Run(test.v+"/"+test.defaultPort, func(t *testing.T) {
			addr, port, err := addr.SplitHostPort(test.v, test.defaultPort)
			if !testutils.IsError(err, test.err) {
				t.Fatalf("error: expected %q, got: %+v", test.err, err)
			}
			if test.err != "" {
				return
			}
			if addr != test.addr {
				t.Errorf("addr: expected %q, got %q", test.addr, addr)
			}
			if port != test.port {
				t.Errorf("addr: expected %q, got %q", test.port, port)
			}
		})
	}
}
