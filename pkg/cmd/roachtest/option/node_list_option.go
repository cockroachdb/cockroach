// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package option

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

// A NodeListOption is a slice of roachprod node identifiers. The first node is
// assigned 1, the second 2, and so on.
type NodeListOption []int

// NewNodeListOptionRange returns a NodeListOption between start and end
// inclusive.
func NewNodeListOptionRange(start, end int) NodeListOption {
	ret := make(NodeListOption, (end-start)+1)
	for i := 0; i <= (end - start); i++ {
		ret[i] = start + i
	}
	return ret
}

// Equals returns true if the nodes are an exact match.
func (n NodeListOption) Equals(o NodeListOption) bool {
	if len(n) != len(o) {
		return false
	}
	for i := range n {
		if n[i] != o[i] {
			return false
		}
	}
	return true
}

// Option implements Option.
func (n NodeListOption) Option() {}

// Merge merges two NodeListOptions.
func (n NodeListOption) Merge(o NodeListOption) NodeListOption {
	t := make(NodeListOption, 0, len(n)+len(o))
	t = append(t, n...)
	t = append(t, o...)
	if len(t) == 0 {
		return t
	}
	sort.Ints(t)
	r := t[:1]
	for i := 1; i < len(t); i++ {
		if r[len(r)-1] != t[i] {
			r = append(r, t[i])
		}
	}
	return r
}

// RandNode returns a random node from the NodeListOption.
func (n NodeListOption) RandNode() NodeListOption {
	return NodeListOption{n[rand.Intn(len(n))]}
}

// SeededRandNode returns a random node from the NodeListOption using a seeded rand object.
func (n NodeListOption) SeededRandNode(rand *rand.Rand) NodeListOption {
	return NodeListOption{n[rand.Intn(len(n))]}
}

func (n NodeListOption) SeededRandList(rand *rand.Rand, size int) (NodeListOption, error) {
	if size > len(n) {
		return NodeListOption{}, fmt.Errorf("cannot select list - size: %d > len: %d", size, len(n))
	}

	nodes := append([]int{}, n...)
	rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })
	return nodes[:size], nil
}

// NodeIDsString returns the nodes in the NodeListOption, separated by spaces.
func (n NodeListOption) NodeIDsString() string {
	result := ""
	for _, i := range n {
		result += fmt.Sprintf("%s ", strconv.Itoa(i))
	}
	return result
}

// String implements fmt.Stringer.
func (n NodeListOption) String() string {
	if len(n) == 0 {
		return ""
	}

	var buf bytes.Buffer
	buf.WriteByte(':')

	appendRange := func(start, end int) {
		if buf.Len() > 1 {
			buf.WriteByte(',')
		}
		if start == end {
			fmt.Fprintf(&buf, "%d", start)
		} else {
			fmt.Fprintf(&buf, "%d-%d", start, end)
		}
	}

	start, end := -1, -1
	for _, i := range n {
		if start != -1 && end == i-1 {
			end = i
			continue
		}
		if start != -1 {
			appendRange(start, end)
		}
		start, end = i, i
	}
	if start != -1 {
		appendRange(start, end)
	}
	return buf.String()
}

// InstallNodes converts the NodeListOption to install.Nodes
func (n NodeListOption) InstallNodes() install.Nodes {
	installNodes := make(install.Nodes, 0, len(n))
	for _, i := range n {
		installNodes = append(installNodes, install.Node(i))
	}
	return installNodes
}

// FromInstallNodes converts install.Nodes to NodeListOption
func FromInstallNodes(installNodes install.Nodes) NodeListOption {
	nodes := NodeListOption{}
	for _, n := range installNodes {
		nodes = append(nodes, int(n))
	}
	return nodes
}
