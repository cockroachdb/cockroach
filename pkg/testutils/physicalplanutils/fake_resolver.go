// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physicalplanutils

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// FakeResolverForTestCluster creates a fake span resolver for the nodes in a
// test cluster.
func FakeResolverForTestCluster(tc serverutils.TestClusterInterface) physicalplan.SpanResolver {
	nodeDescs := make([]*roachpb.NodeDescriptor, tc.NumServers())
	for i := range nodeDescs {
		s := tc.Server(i)
		nodeDescs[i] = &roachpb.NodeDescriptor{
			NodeID:  s.NodeID(),
			Address: util.UnresolvedAddr{AddressField: s.AdvRPCAddr()},
		}
	}

	return physicalplan.NewFakeSpanResolver(nodeDescs)
}
