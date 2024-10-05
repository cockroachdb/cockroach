// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadindexrec

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TODO: Add nullsOrder once it can be specified in index recommendation
type indexedColumn struct {
	column    tree.Name
	direction tree.Direction
}

// TrieNode is an implementation of the node of a IndexTrie-tree.
//
// TrieNode stores the indexed columns, storing columns, parent node and the
// indexed column represented by the node (used to assign storings).
type indexTrieNode struct {
	children map[indexedColumn]*indexTrieNode
	storing  map[string]struct{}
	parent   *indexTrieNode
	col      indexedColumn
}

// indexTrie is an implementation of a indexTrie-tree specific for indexes of
// one table.
//
// indexTrie stores all the indexed columns in each node with/without storing
// columns, allowing Insert, RemoveStorings, AssignStoring. Check the
// corresponding methods for more details.
type indexTrie struct {
	root *indexTrieNode
}

// NewTrie returns a new trie tree.
func NewTrie() *indexTrie {
	return &indexTrie{
		root: &indexTrieNode{},
	}
}

// Insert parses the columns in ci (CreateIndex) and updates the trie.
func (trie *indexTrie) Insert(indexedCols tree.IndexElemList, storingCols tree.NameList) {
	node := trie.root
	for _, indexedCol := range indexedCols {
		indexCol := indexedColumn{
			column:    indexedCol.Column,
			direction: indexedCol.Direction,
		}

		// To avoid duplicate branches, there are only 2 cases for child nodes with
		// the same column, (col1, ASC), (col1, DESC). The (col1, Default) is the
		// same as (col1, ASC).
		if indexCol.direction == tree.DefaultDirection {
			indexCol.direction = tree.Ascending
		}

		if node.children == nil {
			node.children = make(map[indexedColumn]*indexTrieNode)
		}

		if _, ok := node.children[indexCol]; !ok {
			node.children[indexCol] = &indexTrieNode{
				parent: node,
				col:    indexCol,
			}
		}
		node = node.children[indexCol]
	}

	if len(storingCols) > 0 {
		if node.storing == nil {
			node.storing = make(map[string]struct{})
		}
		for _, storingCol := range storingCols {
			node.storing[string(storingCol)] = struct{}{}
		}
	}
}

// RemoveStorings removes those storings that are covered by the leaf nodes.
// It iterates the whole trie for each table by breadth-first search (BFS).
// Whenever there is a node with storing, it will invoke the removeStoringCoveredByLeaf
// to check whether there exists a leaf node covering its storing.
func (trie *indexTrie) RemoveStorings() {
	var queue []*indexTrieNode
	queue = append(queue, trie.root)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if len(node.storing) > 0 {
			if node.removeStoringCoveredByLeaf(node.storing) {
				node.storing = make(map[string]struct{})
			}
		}
		for _, child := range node.children {
			queue = append(queue, child)
		}
	}
}

// removeStoringCoveredByLeaf checks whether the storing is covered by the leaf
// nodes by depth-first search (DFS).
// TODO: Think of algorithm to remove subset of the storing part.
func (node *indexTrieNode) removeStoringCoveredByLeaf(restStoring map[string]struct{}) bool {
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

		if child.removeStoringCoveredByLeaf(restStoring) {
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

// AssignStoring assigns the storings for all the tables in tm, see
// assignStoringToShallowestLeaf for its detailed functionality.
func (trie *indexTrie) AssignStoring() {
	trie.root.assignStoringToShallowestLeaf(0)
}

// assignStoringToShallowestLeaf assign the storing of each node to
// the shallowest leaf node inside its subtree.
func (node *indexTrieNode) assignStoringToShallowestLeaf(curDep int) (*indexTrieNode, int) {
	if len(node.children) == 0 {
		return node, curDep
	}

	var shallowLeaf *indexTrieNode
	// largest depth
	dep := math.MaxInt64
	for _, child := range node.children {
		tempLeaf, tempDep := child.assignStoringToShallowestLeaf(curDep + 1)
		if tempDep < dep {
			dep = tempDep
			shallowLeaf = tempLeaf
		}
	}

	// Assign the storing of node to the shallowLeaf, some columns may be covered
	// along the path from node to the shallowLeaf. As shown in the example below:
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
		tempNode := shallowLeaf
		for tempNode != node {
			delete(node.storing, string(tempNode.col.column))
			tempNode = tempNode.parent
		}

		if len(node.storing) > 0 {
			if shallowLeaf.storing == nil {
				shallowLeaf.storing = make(map[string]struct{})
			}
			for col := range node.storing {
				shallowLeaf.storing[col] = struct{}{}
			}
			node.storing = nil
		}
	}

	return shallowLeaf, dep
}

// collectAllLeavesForTables collects all the indexes represented by the leaf
// nodes of trie.
func collectAllLeavesForTable(trie *indexTrie) ([][]indexedColumn, [][]tree.Name) {
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
