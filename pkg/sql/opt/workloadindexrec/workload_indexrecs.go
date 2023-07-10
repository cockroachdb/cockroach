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
	"context"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// FindWorkloadRecs finds index recommendations for the whole workload after the timestamp ts
// within the space budget represented by budgetBytes.
func FindWorkloadRecs(
	ctx context.Context, evalCtx *eval.Context, ts *tree.DTimestampTZ, budgetBytes int64,
) ([]string, error) {
	cis, dis, err := collectIndexRecs(ctx, evalCtx, ts)
	if err != nil {
		return nil, err
	}

	trieMap := buildTrieForIndexRecs(cis)
	newCis := extractIndexCovering(trieMap)

	var res = make([]string, len(newCis))

	var idx = 0
	for _, ci := range newCis {
		res[idx] = ci.String() + ";"
		idx++
	}

	// Since we collect all the indexes represented by the leaf nodes, all the indexes with
	// "DROP INDEX" has been covered, so we can directly drop all of them without duplicates.
	var disMap = make(map[tree.TableIndexName]bool)
	for _, di := range dis {
		for _, index := range di.IndexList {
			disMap[*index] = true
		}
	}

	for index := range disMap {
		dropCmd := tree.DropIndex{
			IndexList: []*tree.TableIndexName{&index},
		}
		res = append(res, dropCmd.String()+";")
	}

	return res, nil
}

// collectIndexRecs collects all the index recommendations stored in the system.statement_statistics
// with the time later than ts.
func collectIndexRecs(
	ctx context.Context, evalCtx *eval.Context, ts *tree.DTimestampTZ,
) ([]tree.CreateIndex, []tree.DropIndex, error) {
	query := `SELECT index_recommendations FROM system.statement_statistics
						 WHERE (statistics -> 'statistics' ->> 'lastExecAt')::TIMESTAMPTZ > $1
						 AND array_length(index_recommendations, 1) > 0;`
	indexRecs, err := evalCtx.Planner.QueryIteratorEx(ctx, "get-candidates-for-workload-indexrecs",
		sessiondata.NoSessionDataOverride, query, ts.Time)
	if err != nil {
		return nil, nil, err
	}

	// Since Alter index only makes invisible indexes visible, skip it for now.
	var p parser.Parser
	var cis []tree.CreateIndex
	var dis []tree.DropIndex
	var ok bool

	// The index recommendation starts with "creation", "replacement" or "alteration"
	var r = regexp.MustCompile(`\s*(creation|replacement|alteration)\s*:\s*(.*)`)

	for ok, err = indexRecs.Next(ctx); ok; ok, err = indexRecs.Next(ctx) {
		if !ok || err != nil {
			continue
		}

		indexes := tree.MustBeDArray(indexRecs.Cur()[0])
		for _, index := range indexes.Array {
			indexStr, ok := index.(*tree.DString)
			if !ok {
				fmt.Println(index.String() + " is not a string!")
				continue
			}

			indexStrArr := r.FindStringSubmatch(string(*indexStr))
			if indexStrArr == nil {
				fmt.Println(string(*indexStr) + " is not a valid index recommendation!")
				continue
			}

			// Ignore all the alter index recommendations right now
			if indexStrArr[1] == "alteration" {
				continue
			}

			stmts, err := p.Parse(indexStrArr[2])
			if err != nil {
				fmt.Println(indexStrArr[2] + " is not a valid index operation!")
				continue
			}

			for _, stmt := range stmts {
				switch stmt := stmt.AST.(type) {
				case *tree.CreateIndex:
					// ignore all the inverted, partial and sharded indexes right now
					if !stmt.Inverted && stmt.Predicate == nil && stmt.Sharded == nil {
						cis = append(cis, *stmt)
					}
				case *tree.DropIndex:
					dis = append(dis, *stmt)
				}
			}
		}
	}

	return cis, dis, nil
}

// buildTrieForIndexRecs builds the relation among all the indexRecs by a trie tree.
func buildTrieForIndexRecs(cis []tree.CreateIndex) map[tree.TableName]*Trie {
	trieMap := make(map[tree.TableName]*Trie)
	for _, ci := range cis {
		if _, ok := trieMap[ci.Table]; !ok {
			trieMap[ci.Table] = NewTrie()
		}

		trieMap[ci.Table].Insert(ci)
	}
	return trieMap
}

// extractIndexCovering pushs down the storing part of the internal nodes: find whether it
// is covered by some leaf nodes. If yes, discard it; Otherwise, assign it to the shallowest
// leaf node. Then extractIndexCovering collects all the indexes represented by the leaf node.
func extractIndexCovering(tm map[tree.TableName]*Trie) []tree.CreateIndex {
	removeStorings(tm)
	assignStoring(tm)
	return collectAllLeaves4Tables(tm)
}
