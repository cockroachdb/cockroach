// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// ListNodes parses and validates a node selector string, which is either "all"
// (indicating all nodes in the cluster) or a comma-separated list of nodes and
// node ranges. Nodes are 1-indexed.
//
// Examples:
//  - "all"
//  - "1"
//  - "1-3"
//  - "1,3,5"
//  - "1,2-4,7-8"
//
func ListNodes(s string, numNodesInCluster int) ([]int, error) {
	if s == "" {
		return nil, errors.AssertionFailedf("empty node selector")
	}
	if numNodesInCluster < 1 {
		return nil, errors.AssertionFailedf("invalid number of nodes %d", numNodesInCluster)
	}

	if s == "all" {
		return allNodes(numNodesInCluster), nil
	}

	var set util.FastIntSet
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
	nodes := set.Ordered()
	// Check that the values make sense.
	for _, n := range nodes {
		if n < 1 {
			return nil, fmt.Errorf("invalid node selector '%s', node values start at 1", s)
		}
		if n > numNodesInCluster {
			return nil, fmt.Errorf(
				"invalid node selector '%s', cluster contains %d nodes", s, numNodesInCluster,
			)
		}
	}
	return nodes, nil
}

func allNodes(numNodesInCluster int) []int {
	r := make([]int, numNodesInCluster)
	for i := range r {
		r[i] = i + 1
	}
	return r
}
