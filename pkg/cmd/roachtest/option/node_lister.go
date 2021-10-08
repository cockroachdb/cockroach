// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

// NodeLister is a helper to create `option.NodeListOption`s.
type NodeLister struct {
	NodeCount int
	Fatalf    func(string, ...interface{})
}

// All returns a list of all nodes.
func (l NodeLister) All() NodeListOption {
	return l.Range(1, l.NodeCount)
}

// Range returns only the nodes [begin, ..., end].
func (l NodeLister) Range(begin, end int) NodeListOption {
	if begin < 1 || end > l.NodeCount {
		l.Fatalf("invalid node range: %d-%d (1-%d)", begin, end, l.NodeCount)
		return nil
	}
	r := make(NodeListOption, 0, 1+end-begin)
	for i := begin; i <= end; i++ {
		r = append(r, i)
	}
	return r
}

// Nodes returns only the nodes at the provided (1-indexed) positions.
func (l NodeLister) Nodes(ns ...int) NodeListOption {
	r := make(NodeListOption, 0, len(ns))
	for _, n := range ns {
		if n < 1 || n > l.NodeCount {
			l.Fatalf("invalid node range: %d (1-%d)", n, l.NodeCount)
		}

		r = append(r, n)
	}
	return r
}

// Node returns only the node at the provided (1-indexed) position.
func (l NodeLister) Node(n int) NodeListOption {
	return l.Nodes(n)
}
