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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
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
