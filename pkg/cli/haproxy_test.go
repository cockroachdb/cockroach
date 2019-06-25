// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNodeStatusToNodeInfoConversion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input    []statuspb.NodeStatus
		expected []haProxyNodeInfo
	}{
		{
			[]statuspb.NodeStatus{
				{
					Desc: roachpb.NodeDescriptor{
						NodeID: 1,
						Address: util.UnresolvedAddr{
							AddressField: "addr",
						},
					},
					// Flags but no http port.
					Args: []string{"--unwanted", "-unwanted"},
				},
			},
			[]haProxyNodeInfo{
				{
					NodeID:    1,
					NodeAddr:  "addr",
					CheckPort: base.DefaultHTTPPort,
				},
			},
		},
		{
			[]statuspb.NodeStatus{
				{
					Args: []string{"--unwanted", "--http-port=1234"},
				},
				{
					Args: nil,
				},
				{
					Args: []string{"--http-addr=foo:4567"},
				},
			},
			[]haProxyNodeInfo{
				{
					CheckPort: "1234",
				},
				{
					CheckPort: base.DefaultHTTPPort,
				},
				{
					CheckPort: "4567",
				},
			},
		},
		{
			[]statuspb.NodeStatus{
				{
					Args: []string{"--http-port", "5678", "--unwanted"},
				},
			},
			[]haProxyNodeInfo{
				{
					CheckPort: "5678",
				},
			},
		},
		{
			[]statuspb.NodeStatus{
				{
					// We shouldn't see this, because the flag needs an argument on startup,
					// but check that we fall back to the default port.
					Args: []string{"-http-port"},
				},
			},
			[]haProxyNodeInfo{
				{
					CheckPort: base.DefaultHTTPPort,
				},
			},
		},
	}

	for _, testCase := range testCases {
		output := nodeStatusesToNodeInfos(testCase.input)
		if !reflect.DeepEqual(output, testCase.expected) {
			t.Fatalf("unexpected output %v, expected %v", output, testCase.expected)
		}
	}
}

func TestMatchLocalityRegexp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		locality string // The locality as passed to `start --locality=xx`
		desired  string // The desired locality as passed to `gen haproxy --locality=xx`
		matches  bool
	}{
		{"", "", true},
		{"region=us-east1", "", true},
		{"country=us,region=us-east1,datacenter=blah", "", true},
		{"country=us,region=us-east1,datacenter=blah", "country=us", true},
		{"country=us,region=us-east1,datacenter=blah", "count.*=us", false},
		{"country=us,region=us-east1,datacenter=blah", "country=u", false},
		{"country=us,region=us-east1,datacenter=blah", "try=us", false},
		{"country=us,region=us-east1,datacenter=blah", "region=us-east1", true},
		{"country=us,region=us-east1,datacenter=blah", "region=us.*", true},
		{"country=us,region=us-east1,datacenter=blah", "region=us.*,country=us", true},
		{"country=us,region=us-east1,datacenter=blah", "region=notus", false},
		{"country=us,region=us-east1,datacenter=blah", "something=else", false},
		{"country=us,region=us-east1,datacenter=blah", "region=us.*,zone=foo", false},
	}

	for testNum, testCase := range testCases {
		// We're not testing locality parsing: fail on error.
		var locality, desired roachpb.Locality
		if testCase.locality != "" {
			if err := locality.Set(testCase.locality); err != nil {
				t.Fatal(err)
			}
		}
		if testCase.desired != "" {
			if err := desired.Set(testCase.desired); err != nil {
				t.Fatal(err)
			}
		}
		matches, _ := localityMatches(locality, desired)
		if matches != testCase.matches {
			t.Errorf("#%d: expected match %t, got %t", testNum, testCase.matches, matches)
		}
	}
}
