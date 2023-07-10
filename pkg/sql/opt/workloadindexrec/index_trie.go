// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workloadindexrec

import (
	"container/list"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type indexedColumn struct {
	column     tree.Name
	direction  tree.Direction
	nullsOrder tree.NullsOrder
}

// TrieNode is an implementation of the node of a IndexTrie-tree.
//
// TrieNode stores the indexed columns, storing columns, parent node and the indexed
// column represented by the node (used to assign storings).
type indexTrieNode struct {
	children map[indexedColumn]*indexTrieNode
	storing  map[string]struct{}
	parent   *indexTrieNode
	col      indexedColumn
}

// IndexTrie is an implementation of a IndexTrie-tree specific for indexes of one table.
//
// IndexTrie stores all the indexed columns in each node with/without storing columns,
// allowing insertion, removeStorings, assignStoring. Check the corresponding
// methods for more details.
type IndexTrie struct {
	root *indexTrieNode
}

// NewTrie returns a new trie tree.
func NewTrie() *IndexTrie {
	return &IndexTrie{
		root: &indexTrieNode{
			children: make(map[indexedColumn]*indexTrieNode),
			storing:  make(map[string]struct{}),
			parent:   nil,
			col:      indexedColumn{},
		},
	}
}

// Insert parses the columns in ci (CreateIndex) and updates the trie.
func (t *IndexTrie) insert(indexedCols tree.IndexElemList, storingCols tree.NameList) {
	node := t.root
	for _, indexedCol := range indexedCols {
		indexCol := indexedColumn{
			column:     indexedCol.Column,
			direction:  indexedCol.Direction,
			nullsOrder: indexedCol.NullsOrder,
		}
		if indexCol.direction == tree.DefaultDirection {
			indexCol.direction = tree.Ascending
			if indexCol.nullsOrder == tree.DefaultNullsOrder {
				indexCol.nullsOrder = tree.NullsFirst
			}
		} else if indexCol.direction == tree.Descending {
			if indexCol.nullsOrder == tree.DefaultNullsOrder {
				indexCol.nullsOrder = tree.NullsLast
			}
		}

		if _, ok := node.children[indexCol]; !ok {
			node.children[indexCol] = &indexTrieNode{
				children: make(map[indexedColumn]*indexTrieNode),
				storing:  make(map[string]struct{}),
				parent:   node,
				col:      indexCol,
			}
		}
		node = node.children[indexCol]
	}
	for _, storingCol := range storingCols {
		node.storing[string(storingCol)] = struct{}{}
	}
}

// removeStorings removes those storings that are covered by the leaf nodes.
// It iterates the whole trie for each table by breadth-first search (BFS).
// Whenever there is a node with storing, it will invoke the removeStoringCoveredByLeaf
// to check whether there exists a leaf node covering its storing.
func removeStorings(trie *IndexTrie) {
	queue := list.New()
	queue.PushBack(trie.root)
	for queue.Len() > 0 {
		node := queue.Front().Value.(*indexTrieNode)
		queue.Remove(queue.Front())
		if len(node.storing) > 0 {
			if removeStoringCoveredByLeaf(node, node.storing) {
				node.storing = make(map[string]struct{})
			}
		}
		for _, child := range node.children {
			queue.PushBack(child)
		}
	}
}

// removeStoringCoveredByLeaf checks whether the storing is covered by the leaf
// nodes by depth-first search (DFS).
// TODO: Think of algorithm to remove subset of the storing part.
func removeStoringCoveredByLeaf(node *indexTrieNode, restStoring map[string]struct{}) bool {
	// Nothing else we need to cover for the storing even if we have not reached
	// the leaf node.
	if len(restStoring) == 0 {
		return true
	}

	// leaf node
	if len(node.children) == 0 {
		return false
	}

	for indexCol, child := range node.children {
		// Delete the element covered by the child
		var found = false
		if _, ok := restStoring[string(indexCol.column)]; ok {
			found = true
			delete(restStoring, string(indexCol.column))
		}

		if removeStoringCoveredByLeaf(child, restStoring) {
			return true
		}

		// Recover the deleted element so that we can reuse the restStoring for all
		// the children.
		if found {
			restStoring[string(indexCol.column)] = struct{}{}
		}
	}

	return false
}

// assignStoring assigns the storings for all the tables in tm, see
// assignStoringToShallowestLeaf for its detailed functionality.
func assignStoring(trie *IndexTrie) {
	assignStoringToShallowestLeaf(trie.root, 0)
}

// assignStoringToShallowestLeaf assign the storing of each node to
// the shallowest leaf node inside its subtree.
func assignStoringToShallowestLeaf(node *indexTrieNode, curDep int16) (*indexTrieNode, int16) {
	if len(node.children) == 0 {
		return node, curDep
	}

	var shallowLeaf *indexTrieNode
	// largest depth
	var dep int16 = math.MaxInt16
	for _, child := range node.children {
		tempLeaf, tempDep := assignStoringToShallowestLeaf(child, curDep+1)
		if tempDep < dep {
			dep = tempDep
			shallowLeaf = tempLeaf
		}
	}

	// Assign the storing of node to the shallowLeaf, some columns may be
	// covered along the path from node to the shallowLeaf. As shown in the
	// example below:
	//
	//   a storing (b, c, d)
	// /   \
	// c   b
	// |   |
	// e   c
	// |
	// f
	//
	// When we finish traversing the tree and go back to the node "a", the
	// shallowest leaf node will be the right "c". Then we would like to assign
	// the storing part of node "a" to that shallowest leaf node "c". Before
	// assign all of them, we need to remove all the columns that covered by the
	// path from "c" to "a", which is "b" and "c" in this example. The following
	// code traverses from the shallowest leaf node to the current node and remove
	// all the columns covered by the path. Finally, the trie will be like:
	//
	//   a
	// /   \
	// c   b
	// |   |
	// e   c storing (d)
	// |
	// f
	//
	// The right leaf node "c" will represent the index (a, b, c) storing (d).
	if len(node.storing) > 0 {
		var tempNode = shallowLeaf
		for tempNode != node {
			delete(node.storing, string(tempNode.col.column))
			tempNode = tempNode.parent
		}

		if len(node.storing) > 0 {
			for col := range node.storing {
				shallowLeaf.storing[col] = struct{}{}
			}
			node.storing = make(map[string]struct{})
		}
	}

	return shallowLeaf, dep
}

// collectAllLeaves4Tables collects all the indexes represented by the leaf
// nodes of trie.
func collectAllLeaves4Table(trie *IndexTrie) ([][]indexedColumn, [][]tree.Name) {
	var indexedColsArray [][]indexedColumn
	var storingColsArray [][]tree.Name
	collectAllLeaves(trie.root, &indexedColsArray, &storingColsArray, []indexedColumn{})
	return indexedColsArray, storingColsArray
}

// collectAllLeaves collects all the indexes represented by the leaf nodes
// recursively.
func collectAllLeaves(
	node *indexTrieNode,
	indexedCols *[][]indexedColumn,
	storingCols *[][]tree.Name,
	curIndexedCols []indexedColumn,
) {
	if len(node.children) == 0 {
		curStoringCols := make([]tree.Name, len(node.storing))
		var idx = 0
		for storingCol := range node.storing {
			curStoringCols[idx] = tree.Name(storingCol)
			idx++
		}
		*indexedCols = append(*indexedCols, curIndexedCols)
		*storingCols = append(*storingCols, curStoringCols)
		return
	}

	for indexCol, child := range node.children {
		collectAllLeaves(child, indexedCols, storingCols, append(curIndexedCols, indexCol))
	}
}
