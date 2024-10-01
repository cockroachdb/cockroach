// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// Node represents a node in a roachprod cluster; a cluster of N nodes consists
// of nodes 1 through N.
type Node int

// Nodes is a list of nodes.
type Nodes []Node

// ListNodes parses and validates a node selector string, which is either "all"
// (indicating all nodes in the cluster) or a comma-separated list of nodes and
// node ranges. Nodes are 1-indexed.
//
// Examples:
//   - "all"
//   - "1"
//   - "1-3"
//   - "1,3,5"
//   - "1,2-4,7-8"
func ListNodes(s string, numNodesInCluster int) (Nodes, error) {
	if s == "" {
		return nil, errors.AssertionFailedf("empty node selector")
	}
	if numNodesInCluster < 1 {
		return nil, errors.AssertionFailedf("invalid number of nodes %d", numNodesInCluster)
	}

	if s == "all" {
		return allNodes(numNodesInCluster), nil
	}

	var set intsets.Fast
	for _, p := range strings.Split(s, ",") {
		parts := strings.Split(p, "-")
		switch len(parts) {
		case 1:
			i, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse node selector '%s'", s)
			}
			set.Add(i)

		case 2:
			from, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse node selector '%s'", s)
			}
			to, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, errors.Wrapf(err, "unable to parse node selector '%s'", s)
			}
			set.AddRange(from, to)

		default:
			return nil, fmt.Errorf("unable to parse node selector '%s'", p)
		}
	}
	nodes := make(Nodes, 0, set.Len())
	set.ForEach(func(v int) {
		nodes = append(nodes, Node(v))
	})
	// Check that the values make sense.
	for _, n := range nodes {
		if n < 1 {
			return nil, fmt.Errorf("invalid node selector '%s', node values start at 1", s)
		}
		if int(n) > numNodesInCluster {
			return nil, fmt.Errorf(
				"invalid node selector '%s', cluster contains %d nodes", s, numNodesInCluster,
			)
		}
	}
	return nodes, nil
}

func allNodes(numNodesInCluster int) Nodes {
	r := make(Nodes, numNodesInCluster)
	for i := range r {
		r[i] = Node(i + 1)
	}
	return r
}

func (n Nodes) Contains(node Node) bool {
	for _, v := range n {
		if v == node {
			return true
		}
	}
	return false
}
