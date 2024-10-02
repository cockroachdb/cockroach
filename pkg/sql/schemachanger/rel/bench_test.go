// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/stretchr/testify/require"
)

type ListNode struct {
	ID   int
	Next int
}

type llAttrs int

var attrNames = []string{"id", "next"}

func (l llAttrs) String() string { return attrNames[l] }

const (
	idAttr   llAttrs = 0
	nextAttr llAttrs = 1
)

var _ rel.Attr = llAttrs(0)

func forEachListDepth(f func(lists, depth int)) {
	for _, lists := range []int{128, 512, 2048, 8192} {
		for _, depth := range []int{1, 4, 16} {
			f(lists, depth)
		}
	}
}

// BenchmarkLinkedList constructs A linked list of N elements and figures out
// how long it takes to find sublists of A given depth and end point. Note that
// in general, the process of reversing the linked list without any indexing is
// a O(N * depth) proposition. With the indexing, it becomes O(log(N) * depth).
func BenchmarkLinkedList(b *testing.B) {
	sc := rel.MustSchema("bench",
		rel.EntityMapping(reflect.TypeOf((*ListNode)(nil)),
			rel.EntityAttr(idAttr, "ID"),
			rel.EntityAttr(nextAttr, "Next"),
		),
	)
	mkVar := func(i int) rel.Var {
		return rel.Var(strconv.Itoa(i))
	}
	queryDepth := func(depth int) (_ []rel.Clause, endName rel.Var) {
		names := make([]rel.Var, 0, depth+1)
		for i := 0; i < depth+1; i++ {
			names = append(names, mkVar(i))
		}
		idVar := func(nameVar rel.Var) rel.Var {
			return nameVar + "id"
		}
		var terms []rel.Clause
		for i := depth; i > 0; i-- {
			terms = append(terms,
				names[i-1].AttrEqVar(nextAttr, idVar(names[i])),
				names[i].AttrEqVar(idAttr, idVar(names[i])),
			)
		}
		return terms, idVar(names[depth])
	}
	check := func(b *testing.B, q int, depth int, links, perm []int, r rel.Result) error {
		b.StopTimer()
		defer b.StartTimer()
		var exp int
		var ln *ListNode
		for i := 0; i < depth+1; i++ {
			ln = r.Var(mkVar(i)).(*ListNode)
			if i == 0 {
				exp = ln.ID
			}
			require.Equal(b, exp, ln.ID)
			require.Equal(b, links[ln.ID], ln.Next)
			exp = ln.Next
		}
		require.Equal(b, ln.ID, perm[q], "%d %v %d %v", perm, perm[q], ln)
		return nil
	}

	// We want to create N nodes such that each node has attributes
	// ID, next, prev. Then we want to define A set of queries for the
	// specified depth.
	const numQueries = 16
	runDepth := func(b *testing.B, lists, depth int, indexes []rel.Index) {
		links := rand.Perm(lists + depth)
		p := rand.Perm(lists)[:numQueries]
		db, err := rel.NewDatabase(sc, indexes...)
		require.NoError(b, err)
		for i, j := range links {
			require.NoError(b, db.Insert(&ListNode{ID: i, Next: j}))
		}

		queries := make([]*rel.Query, numQueries)
		clauses, endVar := queryDepth(depth)
		for i, end := range p {
			q, err := rel.NewQuery(sc, append(clauses, endVar.Eq(end))...)
			require.NoError(b, err)
			queries[i] = q
		}

		var q int
		run := func(r rel.Result) error {
			const checkFrac = .01 // check that the list is correct sometimes
			if rand.Float64() < checkFrac {
				require.NoError(b, check(b, q, depth, links, p, r))
			}
			return nil
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q = rand.Intn(numQueries)
			require.NoError(b, queries[q].Iterate(db, nil, run))
		}
	}

	for _, indexes := range [][]rel.Index{
		{
			{Attrs: []rel.Attr{nextAttr}},
			{Attrs: []rel.Attr{idAttr}},
		},
		{{}},
	} {
		forEachListDepth(func(lists, depth int) {
			b.Run(fmt.Sprintf("lists=%d,depth=%d,%v", lists, depth, indexes), func(b *testing.B) {
				runDepth(b, lists, depth, indexes)
			})
		})
	}

}

// BenchmarkReverseLinkedListNoRel is used to provide a reminder that while
// the big-O notation of the indexed solution may be better, for an N that
// is smaller than 8k, it's slower than the N^2 approach.
func BenchmarkReverseLinkedListNoRel(b *testing.B) {
	const numQueries = 16
	// Benchmark running the same process of reversing the list without the
	// query structure.
	runDepthNoQuery := func(b *testing.B, lists, depth int) {
		links := rand.Perm(lists + depth)
		p := rand.Perm(lists)[:numQueries]
		nodes := make([]*ListNode, len(links))
		for i, j := range links {
			nodes[i] = &ListNode{ID: i, Next: j}
		}
		res := make([]*ListNode, depth+1)
		query := func(q int) {
			res[depth] = nodes[p[q]]
			for d := depth - 1; d >= 0; d-- {
				for _, n := range nodes {
					if n.Next == res[d+1].ID {
						res[d] = n
						break
					}
				}
			}
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q := rand.Intn(numQueries)
			query(q)
		}
	}
	forEachListDepth(func(lists, depth int) {
		b.Run(fmt.Sprintf("lists=%d,depth=%d", lists, depth), func(b *testing.B) {
			runDepthNoQuery(b, lists, depth)
		})
	})
}
