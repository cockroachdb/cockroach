// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"

type loadGroup struct {
	roachNodes option.NodeListOption
	loadNodes  option.NodeListOption
}

type loadGroupList []loadGroup

func (lg loadGroupList) roachNodes() option.NodeListOption {
	var roachNodes option.NodeListOption
	for _, g := range lg {
		roachNodes = roachNodes.Merge(g.roachNodes)
	}
	return roachNodes
}

func (lg loadGroupList) loadNodes() option.NodeListOption {
	var loadNodes option.NodeListOption
	for _, g := range lg {
		loadNodes = loadNodes.Merge(g.loadNodes)
	}
	return loadNodes
}

// makeLoadGroups create a loadGroupList that has an equal number of cockroach
// nodes per zone. It assumes that numLoadNodes <= numZones and that numZones is
// divisible by numLoadNodes.
func makeLoadGroups(
	c interface {
		Node(int) option.NodeListOption
		Range(int, int) option.NodeListOption
	},
	numZones, numRoachNodes, numLoadNodes int,
) loadGroupList {
	if numLoadNodes > numZones {
		panic("cannot have more than one load node per zone")
	} else if numZones%numLoadNodes != 0 {
		panic("numZones must be divisible by numLoadNodes")
	}
	// roachprod allocates nodes over regions in a round-robin fashion.
	// If the number of nodes is not divisible by the number of regions, the
	// extra nodes are allocated in a round-robin fashion over the regions at
	// the end of cluster.
	loadNodesAtTheEnd := numLoadNodes%numZones != 0
	loadGroups := make(loadGroupList, numLoadNodes)
	roachNodesPerGroup := numRoachNodes / numLoadNodes
	for i := range loadGroups {
		if loadNodesAtTheEnd {
			first := i*roachNodesPerGroup + 1
			loadGroups[i].roachNodes = c.Range(first, first+roachNodesPerGroup-1)
			loadGroups[i].loadNodes = c.Node(numRoachNodes + i + 1)
		} else {
			first := i*(roachNodesPerGroup+1) + 1
			loadGroups[i].roachNodes = c.Range(first, first+roachNodesPerGroup-1)
			loadGroups[i].loadNodes = c.Node((i + 1) * (roachNodesPerGroup + 1))
		}
	}
	return loadGroups
}
