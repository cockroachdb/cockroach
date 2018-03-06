// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package cli

import (
	"testing"

	"reflect"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNodeStatusToNodeInfoConversion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	input := []status.NodeStatus{
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
		{
			Args: []string{"--unwanted", "-http-port=1234"},
		},
		{
			Args: []string{"--http-port", "5678", "--unwanted"},
		},
		{
			// No flags.
			Args: nil,
		},
		{
			// We shouldn't see this, because the flag needs an argument on startup,
			// but check that we fall back to the default port.
			Args: []string{"-http-port"},
		},
	}
	expected := []haProxyNodeInfo{
		{
			NodeID:    1,
			NodeAddr:  "addr",
			CheckPort: base.DefaultHTTPPort,
		},
		{
			CheckPort: "1234",
		},
		{
			CheckPort: "5678",
		},
		{
			CheckPort: base.DefaultHTTPPort,
		},
		{
			CheckPort: base.DefaultHTTPPort,
		},
	}

	output := nodeStatusesToNodeInfos(input)
	for i, o := range output {
		if !reflect.DeepEqual(expected[i], o) {
			t.Fatalf("unexpected output %v, expected %v", o, expected[i])
		}
	}
}
