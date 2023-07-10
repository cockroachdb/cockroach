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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestIndexTrie(t *testing.T) {
	var indexRecStrs []string
	indexRecStrs = append(indexRecStrs, "CREATE INDEX t1_k ON t1(k)")
	var p parser.Parser
	trieMap := make(map[tree.TableName]*indexTrie)

	for _, indexRecStr := range indexRecStrs {
		indexRecs, err := p.Parse(indexRecStr)
		require.Nil(t, err)
		for _, indexRec := range indexRecs {
			switch indexRec := indexRec.AST.(type) {
			case *tree.CreateIndex:
				if !indexRec.Inverted && indexRec.Predicate == nil && indexRec.Sharded == nil {
					if _, ok := trieMap[(*indexRec).Table]; !ok {
						trieMap[(*indexRec).Table] = NewTrie()
					}
					trieMap[(*indexRec).Table].insert(*indexRec)
				}
			}
		}
	}

	for _, trie := range trieMap {
		var indexedCols [][]indexedColumn
		var storingCols [][]tree.Name
		collectAllLeaves(trie.root, &indexedCols, &storingCols, []indexedColumn{})
	}
}
