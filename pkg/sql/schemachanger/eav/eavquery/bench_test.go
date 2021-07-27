// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eavquery_test

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eavquery"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eavtest"
	"github.com/stretchr/testify/require"
)

func init() {
	runtime.MemProfileRate = 0
}

// BenchmarkLinkedList constructs a linked list of N elements and figures out
// how long it takes to find sublists of a given depth and starting point.
func BenchmarkLinkedList(b *testing.B) {

	sc := eavtest.NewSchema([]eavtest.AttributeMetadata{
		{Name: "next", Type: eav.TypeInt64},
		{Name: "id", Type: eav.TypeInt64},
	})
	idAttr := sc.At(1)
	nextAttr := sc.At(0)

	// We want to create N nodes such that each node has attributes
	// ID, next, prev. Then we want to define a set of queries for the
	// specified depth.
	runDepth := func(b *testing.B, lists, depth int, attrs [][]eav.Attribute) {

		db := eav.NewTree(sc, attrs)

		links := rand.Perm(lists + depth)
		for i, j := range links {
			id := eav.Int64(i)
			next := eav.Int64(j)
			v := eav.GetValues()
			v.Set(idAttr, &id)
			v.Set(nextAttr, &next)

			db.Insert(v)
		}

		const numQueries = 16
		queries := make([]*eavquery.Query, numQueries)
		p := rand.Perm(lists)[:numQueries]
		names := make([]string, 0, depth+1)
		for i := 0; i < depth+1; i++ {
			names = append(names, strconv.Itoa(i))
		}
		for i, start := range p {
			queries[i] = eavquery.MustBuild(func(qb eavquery.Builder) {
				for i := 0; i < depth; i++ {
					qb.Entity(names[i]).Constrain(
						nextAttr,
						qb.Entity(names[i+1]).
							Reference(idAttr),
					)
				}
				v := eav.Int64(start)
				qb.Entity("0").Constrain(idAttr, &v)
			})
		}
		var q int
		f := func(r eavquery.Result) error {
			const checkFrac = .01
			if rand.Float64() > checkFrac {
				return nil
			}
			b.StopTimer()
			defer b.StartTimer()

			exp := *r.Entity(names[0]).Get(idAttr).(*eav.Int64)
			require.Equal(b, exp, eav.Int64(p[q]), "%d %v", q, p)
			for _, name := range names {
				id := *r.Entity(name).Get(idAttr).(*eav.Int64)
				require.Equal(b, exp, id)
				next := *r.Entity(name).Get(nextAttr).(*eav.Int64)
				require.Equal(b, links[id], int(next))
				exp = next
			}
			return nil
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q = rand.Intn(numQueries)
			queries[q].Evaluate(db, f)
		}
	}
	for _, attrs := range [][][]eav.Attribute{
		{{idAttr}},
		nil,
	} {
		for _, lists := range []int{32, 64, 128, 256, 512, 1024} {
			for _, depth := range []int{2, 4, 8, 16} {
				b.Run(fmt.Sprintf("lists=%d,depth=%d,%s", lists, depth, attrs), func(b *testing.B) {
					runDepth(b, lists, depth, attrs)
				})
			}
		}
	}
}
