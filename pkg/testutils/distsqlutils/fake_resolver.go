// Copyright 2016 The Cockroach Authors.
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

package distsqlutils

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// FakeResolverForTestCluster creates a fake span resolver for the nodes in a
// test cluster.
func FakeResolverForTestCluster(tc serverutils.TestClusterInterface) distsqlplan.SpanResolver {
	nodeDescs := make([]*roachpb.NodeDescriptor, tc.NumServers())
	for i := range nodeDescs {
		s := tc.Server(i)
		nodeDescs[i] = &roachpb.NodeDescriptor{
			NodeID:  s.NodeID(),
			Address: util.UnresolvedAddr{AddressField: s.ServingAddr()},
		}
	}

	return distsqlplan.NewFakeSpanResolver(nodeDescs)
}
