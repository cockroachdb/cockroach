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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TrieNode is an implementation of the node of a Trie-tree.
//
// TrieNode stores the indexed columns, storing columns, father node and the indexed
// column represented by the node (used for assign storings).
type TrieNode struct {
	Children map[tree.IndexElem]*TrieNode
	Storing  map[string]bool
	Fa       *TrieNode
	Col      tree.IndexElem
}

// Trie is an implementation of a Trie-tree specific for indexes of one table.
//
// Trie stores all the indexed columns in each node with/without storing columns,
// allowing insertion, removeStorings, assignStoring. Check the corresponding
// methods for more details.
type Trie struct {
	Root *TrieNode
}

// NewTrie returns a new trie tree
func NewTrie() *Trie {
	return &Trie{
		Root: &TrieNode{
			Children: make(map[tree.IndexElem]*TrieNode),
			Storing:  make(map[string]bool),
			Fa:       nil,
			Col:      tree.IndexElem{},
		},
	}
}

// Insert parses the columns in ci (CreateIndex) and updates the trie.
func (t *Trie) Insert(ci tree.CreateIndex) {
	node := t.Root
	for _, column := range ci.Columns {
		if _, ok := node.Children[column]; !ok {
			node.Children[column] = &TrieNode{
				Children: make(map[tree.IndexElem]*TrieNode),
				Storing:  make(map[string]bool),
				Fa:       node,
				Col:      column,
			}
		}
		node = node.Children[column]
	}
	for _, column := range ci.Storing {
		node.Storing[string(column)] = true
	}
}

// removeStorings removes those storings that are covered by the leaf nodes.
// It iterates the whole trie for each table by breadth-first search (BFS).
// Whenever there is a node with storing, it will invoke the removeStoringCoveredByLeaf
// to check whether there exists a leaf node covering its storing.
func removeStorings(tm map[tree.TableName]*Trie) {
	queue := list.New()
	for _, t := range tm {
		queue.Init()
		queue.PushBack(t.Root)
		for queue.Len() > 0 {
			node := queue.Front().Value.(*TrieNode)
			queue.Remove(queue.Front())
			if len(node.Storing) > 0 {
				if removeStoringCoveredByLeaf(node, node.Storing) {
					node.Storing = make(map[string]bool)
				}
			}
			for _, child := range node.Children {
				queue.PushBack(child)
			}
		}
	}
}

// removeStoringCoveredByLeaf checks whether the storing is covered by the leaf nodes
// by depth-first search (DFS).
func removeStoringCoveredByLeaf(node *TrieNode, restStoring map[string]bool) bool {
	// nothing else we need to cover for the storing
	// even if we have not reached the leaf node.
	if len(restStoring) == 0 {
		return true
	}

	// leaf node
	if len(node.Children) == 0 {
		return false
	}

	for indexCol, child := range node.Children {
		// delete the element covered by the child
		var found = false
		if _, ok := restStoring[string(indexCol.Column)]; ok {
			found = true
			delete(restStoring, string(indexCol.Column))
		}

		if removeStoringCoveredByLeaf(child, restStoring) {
			return true
		}

		// recover the deleted element so that we can reuse the restStoring
		// for all the children
		if found {
			restStoring[string(indexCol.Column)] = true
		}
	}

	return false
}

// assignStoring assigns the storings for all the tables in tm, see
// assignStoringToShallowestLeaf for its detailed functionality.
func assignStoring(tm map[tree.TableName]*Trie) {
	for _, t := range tm {
		assignStoringToShallowestLeaf(t.Root, 0)
	}
}

// assignStoringToShallowestLeaf assign the storing of each node to
// the shallowest leaf node inside its subtree.
func assignStoringToShallowestLeaf(node *TrieNode, curDep int16) (*TrieNode, int16) {
	if len(node.Children) == 0 {
		return node, curDep
	}

	var shallowLeaf *TrieNode
	// largest depth
	var dep int16 = (1 << 15) - 1
	for _, child := range node.Children {
		tempLeaf, tempDep := assignStoringToShallowestLeaf(child, curDep+1)
		if tempDep < dep {
			dep = tempDep
			shallowLeaf = tempLeaf
		}
	}

	if len(node.Storing) > 0 {
		// Assign the storing of node to the shallowLeaf, some columns
		// may be covered along the path from node to the shallowLeaf
		var tempNode = shallowLeaf
		for tempNode != node {
			delete(node.Storing, string(tempNode.Col.Column))
			tempNode = tempNode.Fa
		}

		if len(node.Storing) > 0 {
			for col := range node.Storing {
				tempNode.Storing[col] = true
			}
		}
	}

	return shallowLeaf, dep
}

// collectAllLeaves4Tables collects all the indexes represented by the leaf
// nodes of all the tries in tm.
func collectAllLeaves4Tables(tm map[tree.TableName]*Trie) []tree.CreateIndex {
	var cis []tree.CreateIndex
	for table, trie := range tm {
		var temp []tree.CreateIndex
		collectAllLeaves(trie.Root, &temp, table, []tree.IndexElem{})
		cis = append(cis, temp...)
	}
	return cis
}

// collectAllLeaves collects all the indexes represented by the leaf nodes recursively
func collectAllLeaves(
	node *TrieNode, cis *[]tree.CreateIndex, tableName tree.TableName, indexedCols []tree.IndexElem,
) {
	if len(node.Children) == 0 {
		storingCols := make([]tree.Name, len(node.Storing))
		var idx = 0
		for storingCol := range node.Storing {
			storingCols[idx] = tree.Name(storingCol)
			idx++
		}
		*cis = append(*cis, tree.CreateIndex{
			Table:    tableName,
			Columns:  indexedCols,
			Storing:  storingCols,
			Unique:   false,
			Inverted: false,
		})
		return
	}

	for indexCol, child := range node.Children {
		collectAllLeaves(child, cis, tableName, append(indexedCols, indexCol))
	}
}
