// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/stretchr/testify/require"
)

func TestTreeBasic(t *testing.T) {
	nodes := []scpb.Node{
		{
			Target: scpb.NewTarget(scpb.Target_ADD, &scpb.Table{
				TableID: 1,
			}),
			Status: scpb.Status_ABSENT,
		},
		{
			Target: scpb.NewTarget(scpb.Target_DROP, &scpb.Table{
				TableID: 2,
			}),
			Status: scpb.Status_ABSENT,
		},
		{
			Target: scpb.NewTarget(scpb.Target_DROP, &scpb.Table{
				TableID: 3,
			}),
			Status: scpb.Status_ABSENT,
		},
		{
			Target: scpb.NewTarget(scpb.Target_ADD, &scpb.Sequence{
				SequenceID: 5,
			}),
			Status: scpb.Status_ABSENT,
		},
	}
	tr := eav.NewTree(scpb.AttrSchema(), nil)
	for i := range nodes {
		tr.Insert(&nodes[i])
	}
	var buf strings.Builder
	buf.WriteString("\n")
	_ = tr.Iterate(nil, func(container eav.Entity) error {
		fmt.Fprintln(&buf, scpb.ToString(container.(scpb.Entity)))
		return nil
	})
	require.Equal(t,
		`
[Sequence: {DescID: 5}, ABSENT, ADD]
[Table: {DescID: 1}, ABSENT, ADD]
[Table: {DescID: 2}, ABSENT, DROP]
[Table: {DescID: 3}, ABSENT, DROP]
`, buf.String())
}

func BenchmarkTree(b *testing.B) {
	type tc struct {
		name       string
		originDist func(b *testing.B, c tc) func() descpb.ID
		refDist    func(b *testing.B, c tc) func() descpb.ID
		mul        int
		attrs      [][]eav.Attribute
	}

	makeNodes := func(n int, origin, ref func() descpb.ID) []*scpb.Node {
		nodes := make([]*scpb.Node, n)
		for i := 0; i < n; i++ {
			nodes[i] = &scpb.Node{
				Target: scpb.NewTarget(scpb.Target_ADD, &scpb.InboundForeignKey{
					OriginID:    origin(),
					ReferenceID: ref(),
				}),
				Status: scpb.Status_ABSENT,
			}
		}
		return nodes
	}
	run := func(b *testing.B, c tc) {
		n := (b.N * c.mul) + 1
		nodes := makeNodes(n, c.originDist(b, c), c.refDist(b, c))
		t := eav.NewTree(scpb.AttrSchema(), c.attrs)
		for _, node := range nodes {
			t.Insert(node)
		}
		perm := rand.Perm(len(nodes))
		vals := make([]eav.Values, len(nodes))
		for i, j := range perm {
			vals[i] = eav.GetValues()
			vals[i].SetFrom(nodes[j], scpb.AttrReferencedDescID, scpb.AttrDescID)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			j := i % n
			var seen bool
			_ = t.Iterate(vals[j], func(container eav.Entity) error {
				if seen = container == nodes[perm[j]]; seen {
					return iterutil.StopIteration()
				}
				return nil
			})
			if !seen {
				b.Fatal("expected to see")
			}
		}
	}
	perMul := func(b *testing.B, c tc) func() descpb.ID {
		return func() descpb.ID {
			return descpb.ID(rand.Intn(b.N))
		}
	}
	unique := func(b *testing.B, c tc) func() descpb.ID {
		n := (b.N * c.mul) + 1
		perm := rand.Perm(n)
		return func() descpb.ID {
			next := perm[0]
			perm = perm[1:]
			return descpb.ID(next)
		}
	}
	for _, c := range []tc{
		{
			name:       "indexed",
			originDist: perMul,
			refDist:    unique,
			attrs:      [][]eav.Attribute{{scpb.AttrReferencedDescID, scpb.AttrDescID}},
		},
		{
			name:       "indexed on unique",
			originDist: perMul,
			refDist:    unique,
			attrs:      [][]eav.Attribute{{scpb.AttrReferencedDescID}},
		},
		{
			name:       "indexed on non-unique",
			originDist: perMul,
			refDist:    unique,
			attrs:      [][]eav.Attribute{{scpb.AttrDescID}},
		},
	} {
		b.Run(c.name, func(b *testing.B) {
			for _, m := range []int{1, 2, 4, 8, 16, 32} {
				c.mul = m
				b.Run(fmt.Sprint(m), func(b *testing.B) {
					run(b, c)
				})
			}
		})
	}
}
