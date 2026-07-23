// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"

type LoadGroup struct {
	RoachNodes option.NodeListOption
	LoadNodes  option.NodeListOption
}

type LoadGroupList []LoadGroup

func (lg LoadGroupList) RoachNodes() option.NodeListOption {
	var roachNodes option.NodeListOption
	for _, g := range lg {
		roachNodes = roachNodes.Merge(g.RoachNodes)
	}
	return roachNodes
}

func (lg LoadGroupList) LoadNodes() option.NodeListOption {
	var loadNodes option.NodeListOption
	for _, g := range lg {
		loadNodes = loadNodes.Merge(g.LoadNodes)
	}
	return loadNodes
}

// MakeLoadGroups create a loadGroupList that has an equal number of cockroach
// nodes per zone. It assumes that numLoadNodes <= numZones and that numZones is
// divisible by numLoadNodes.
func MakeLoadGroups(
	c interface {
		Node(int) option.NodeListOption
		Range(int, int) option.NodeListOption
	},
	numZones, numRoachNodes, numLoadNodes int,
) LoadGroupList {
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
	loadGroups := make(LoadGroupList, numLoadNodes)
	roachNodesPerGroup := numRoachNodes / numLoadNodes
	for i := range loadGroups {
		if loadNodesAtTheEnd {
			first := i*roachNodesPerGroup + 1
			loadGroups[i].RoachNodes = c.Range(first, first+roachNodesPerGroup-1)
			loadGroups[i].LoadNodes = c.Node(numRoachNodes + i + 1)
		} else {
			first := i*(roachNodesPerGroup+1) + 1
			loadGroups[i].RoachNodes = c.Range(first, first+roachNodesPerGroup-1)
			loadGroups[i].LoadNodes = c.Node((i + 1) * (roachNodesPerGroup + 1))
		}
	}
	return loadGroups
}
