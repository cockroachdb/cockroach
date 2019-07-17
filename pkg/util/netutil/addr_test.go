// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package netutil_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
)

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
	}

	for _, test := range testData {
		t.Run(test.v+"/"+test.defaultPort, func(t *testing.T) {
			addr, port, err := netutil.SplitHostPort(test.v, test.defaultPort)
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
