// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

type (
	// nodeSet is a set of nodes.
	nodeSet map[install.Node]struct{}
	// instanceMap is a map of SQL instances to a map of nodes that are running
	// that instance.
	instanceMap map[int]nodeSet
	// processMap is a map of virtual cluster names to instance maps. It's a
	// convenience type that allows grouping the processes that should be
	// started and stopped together.
	processMap map[string]instanceMap
)

func (m *processMap) add(virtualClusterName string, instance int, node install.Node) {
	if virtualClusterName == "" {
		virtualClusterName = install.SystemInterfaceName
	}
	if _, ok := (*m)[virtualClusterName]; !ok {
		(*m)[virtualClusterName] = make(map[int]nodeSet, 0)
	}
	if _, ok := (*m)[virtualClusterName][instance]; !ok {
		(*m)[virtualClusterName][instance] = make(nodeSet, 0)
	}
	(*m)[virtualClusterName][instance][node] = struct{}{}
}

// getStartOrder returns the order in which the processes should be started. It
// ensures that the System interface is started first.
func (m *processMap) getStartOrder() []string {
	var order []string
	// If the System interface is present, it should be the first to start.
	if _, ok := (*m)[install.SystemInterfaceName]; ok {
		order = append(order, install.SystemInterfaceName)
	}
	for virtualClusterName := range *m {
		if virtualClusterName != install.SystemInterfaceName {
			order = append(order, virtualClusterName)
		}
	}
	return order
}

// getStopOrder returns the order in which the processes should be stopped.
func (m *processMap) getStopOrder() []string {
	order := m.getStartOrder()
	rand.Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})
	return order
}
