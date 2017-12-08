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

package treeprinter

import (
	"bytes"
	"fmt"
)

var (
	edgeLink = []rune(" │")
	edgeMid  = []rune(" ├── ")
	edgeLast = []rune(" └── ")
)

// Node is a handle associated with a specific depth in a tree. See below for
// sample usage.
type Node struct {
	tree  *tree
	level int
}

// New creates a tree printer and returns a sentinel node reference which
// should be used to add the root. Sample usage:
//
//   tp := New()
//   root := n.Child("root")
//   root.Child("child-1")
//   root.Child("child-2").Child("grandchild")
//   root.Child("child-3")
//
//   fmt.Print(tp.String())
//
// Output:
//
//   root
//    ├── child-1
//    ├── child-2
//    │    └── grandchild
//    └── child-4
//
// Note that the Child calls can't be rearranged arbitrarily; they have
// to be in the order they need to be displayed (depth-first pre-order).
func New() Node {
	return Node{
		tree:  &tree{},
		level: 0,
	}
}

type tree struct {
	// rows maintains the rows accumulated so far, as rune arrays.
	//
	// When a new child is added (e.g. child2 above), we may have to
	// go back up and fix edges.
	rows [][]rune

	// row index of the last row for a given level. Grows as needed.
	lastNode []int
}

// Childf adds a node as a child of the given node.
func (n Node) Childf(format string, args ...interface{}) Node {
	return n.Child(fmt.Sprintf(format, args...))
}

// Child adds a node as a child of the given node.
func (n Node) Child(text string) Node {
	runes := []rune(text)

	// Each level indents by this much.
	k := len(edgeLast)
	indent := n.level * k
	row := make([]rune, indent+len(runes))
	for i := 0; i < indent-k; i++ {
		row[i] = ' '
	}
	if indent >= k {
		copy(row[indent-k:], edgeLast)
	}
	copy(row[indent:], runes)

	for len(n.tree.lastNode) <= n.level+1 {
		n.tree.lastNode = append(n.tree.lastNode, -1)
	}
	n.tree.lastNode[n.level+1] = -1

	if last := n.tree.lastNode[n.level]; last != -1 {
		if n.level == 0 {
			panic("multiple root nodes")
		}
		// Connect to the previous sibling.
		copy(n.tree.rows[last][indent-k:], edgeMid)
		for i := last + 1; i < len(n.tree.rows); i++ {
			copy(n.tree.rows[i][indent-k:], edgeLink)
		}
	}

	n.tree.lastNode[n.level] = len(n.tree.rows)
	n.tree.rows = append(n.tree.rows, row)

	// Return a TreePrinter that can be used for children of this node.
	return Node{
		tree:  n.tree,
		level: n.level + 1,
	}
}

func (n Node) String() string {
	if n.level != 0 {
		panic("Only the root can be stringified")
	}
	var buf bytes.Buffer
	for _, r := range n.tree.rows {
		buf.WriteString(string(r))
		buf.WriteByte('\n')
	}
	return buf.String()
}
