// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package gossip

import (
	"testing"

	"github.com/cockroachdb/cockroach/util"
)

func TestParseResolverSpec(t *testing.T) {
	testCases := []struct {
		input           string
		success         bool
		resolverType    string
		resolverAddress string
	}{
		// Ports are not checked at parsing time. They are at GetAddress time though.
		{"127.0.0.1:8080", true, "tcp", "127.0.0.1:8080"},
		{":8080", true, "tcp", util.EnsureHost(":8080")},
		{"127.0.0.1", true, "tcp", "127.0.0.1"},
		{"tcp=127.0.0.1", true, "tcp", "127.0.0.1"},
		{"lb=127.0.0.1", true, "lb", "127.0.0.1"},
		{"unix=/tmp/unix-socket12345", true, "unix", "/tmp/unix-socket12345"},
		{"", false, "", ""},
		{"foo=127.0.0.1", false, "", ""},
		{"lb=", false, "", ""},
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
		resolverSpec string
		success      bool
		oneShot      bool
		addressType  string
		addressValue string
	}{
		{"tcp=127.0.0.1:8080", true, true, "tcp", "127.0.0.1:8080"},
		{"tcp=127.0.0.1", false, false, "", ""},
		{"tcp=localhost:80", true, true, "tcp", "localhost:80"},
		// We should test unresolvable dns too, but this would be fragile.
		{"lb=localhost:80", true, false, "tcp", "localhost:80"},
		{"lb=127.0.0.1:80", true, false, "tcp", "127.0.0.1:80"},
		{"lb=127.0.0.1", false, false, "", ""},
		{"unix=/tmp/foo", true, true, "unix", "/tmp/foo"},
	}

	for tcNum, tc := range testCases {
		resolver, err := NewResolver(tc.resolverSpec)
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
		if resolver.IsExhausted() != tc.oneShot {
			t.Errorf("#%d: expected exhausted resolver=%t, but is: %+v", tcNum, tc.oneShot, resolver)
		}
		if address.Network() != tc.addressType {
			t.Errorf("#%d: expected address type=%s, got %+v", tcNum, tc.addressType, address)
		}
		if address.String() != tc.addressValue {
			t.Errorf("#%d: expected address value=%s, got %+v", tcNum, tc.addressValue, address)
		}
	}
}
