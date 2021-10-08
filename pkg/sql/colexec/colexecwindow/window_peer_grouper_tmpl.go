// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for window_peer_grouper.eg.go. It's
// formatted in a special way, so it's both valid Go and a valid text/template
// input. This permits editing this file with editor support.
//
// */}}

package colexecwindow

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewWindowPeerGrouper creates a new Operator that puts 'true' in
// outputColIdx'th column (which is appended if needed) for every tuple that is
// the first within its peer group. Peers are tuples that belong to the same
// partition and are equal on the ordering columns. If orderingCols is empty,
// then all tuples within the partition are peers.
// - partitionColIdx, if not columnOmitted, *must* specify the column in which
//   'true' indicates the start of a new partition.
// NOTE: the input *must* already be ordered on ordCols.
func NewWindowPeerGrouper(
	allocator *colmem.Allocator,
	input colexecop.Operator,
	typs []*types.T,
	orderingCols []execinfrapb.Ordering_Column,
	partitionColIdx int,
	outputColIdx int,
) (op colexecop.Operator, err error) {
	allPeers := len(orderingCols) == 0
	var distinctCol []bool
	if !allPeers {
		orderIdxs := make([]uint32, len(orderingCols))
		for i, ordCol := range orderingCols {
			orderIdxs[i] = ordCol.ColIdx
		}
		input, distinctCol, err = colexecbase.OrderedDistinctColsToOperators(
			input, orderIdxs, typs, false, /* nullsAreDistinct */
		)
		if err != nil {
			return nil, err
		}
	}
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, outputColIdx)
	initFields := windowPeerGrouperInitFields{
		OneInputHelper:  colexecop.MakeOneInputHelper(input),
		allocator:       allocator,
		partitionColIdx: partitionColIdx,
		distinctCol:     distinctCol,
		outputColIdx:    outputColIdx,
	}
	if allPeers {
		if partitionColIdx != tree.NoColumnIdx {
			return &windowPeerGrouperAllPeersWithPartitionOp{
				windowPeerGrouperInitFields: initFields,
			}, nil
		}
		return &windowPeerGrouperAllPeersNoPartitionOp{
			windowPeerGrouperInitFields: initFields,
		}, nil
	}
	if partitionColIdx != tree.NoColumnIdx {
		return &windowPeerGrouperWithPartitionOp{
			windowPeerGrouperInitFields: initFields,
		}, nil
	}
	return &windowPeerGrouperNoPartitionOp{
		windowPeerGrouperInitFields: initFields,
	}, nil
}

type windowPeerGrouperInitFields struct {
	colexecop.OneInputHelper

	allocator       *colmem.Allocator
	partitionColIdx int
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which 'true' will indicate that a new peer group begins with
	// the corresponding tuple.
	distinctCol  []bool
	outputColIdx int
}

// {{range .}}

type _PEER_GROUPER_STRINGOp struct {
	windowPeerGrouperInitFields
	// {{if and .AllPeers (not .HasPartition)}}
	seenFirstTuple bool
	// {{end}}
}

var _ colexecop.Operator = &_PEER_GROUPER_STRINGOp{}

func (p *_PEER_GROUPER_STRINGOp) Next() coldata.Batch {
	b := p.Input.Next()
	n := b.Length()
	if n == 0 {
		return b
	}
	// {{if .HasPartition}}
	partitionCol := b.ColVec(p.partitionColIdx).Bool()
	// {{end}}
	sel := b.Selection()
	peersVec := b.ColVec(p.outputColIdx)
	if peersVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		peersVec.Nulls().UnsetNulls()
	}
	peersCol := peersVec.Bool()
	if sel != nil {
		for _, i := range sel[:n] {
			// {{if .AllPeers}}
			// {{if .HasPartition}}
			// All tuples within the partition are peers, so we simply need to copy
			// over partitionCol according to sel.
			peersCol[i] = partitionCol[i]
			// {{else}}
			// There is only one partition and all tuples within it are peers, so we
			// need to set 'true' to only the first tuple ever seen.
			peersCol[i] = !p.seenFirstTuple
			p.seenFirstTuple = true
			// {{end}}
			// {{else}}
			// {{if .HasPartition}}
			// The new peer group begins either when a new partition begins (in which
			// case partitionCol[i] is 'true') or when i'th tuple is different from
			// i-1'th (in which case p.distinctCol[i] is 'true').
			peersCol[i] = partitionCol[i] || p.distinctCol[i]
			// {{else}}
			// The new peer group begins when i'th tuple is different from i-1'th (in
			// which case p.distinctCol[i] is 'true').
			peersCol[i] = p.distinctCol[i]
			// {{end}}
			// {{end}}
		}
	} else {
		// {{if .AllPeers}}
		// {{if .HasPartition}}
		// All tuples within the partition are peers, so we simply need to copy
		// over partitionCol.
		copy(peersCol[:n], partitionCol[:n])
		// {{else}}
		// There is only one partition and all tuples within it are peers, so we
		// need to set 'true' to only the first tuple ever seen.
		copy(peersCol[:n], colexecutils.ZeroBoolColumn[:n])
		peersCol[0] = !p.seenFirstTuple
		p.seenFirstTuple = true
		// {{end}}
		// {{else}}
		// {{if .HasPartition}}
		// The new peer group begins either when a new partition begins (in which
		// case partitionCol[i] is 'true') or when i'th tuple is different from
		// i-1'th (in which case p.distinctCol[i] is 'true').
		_ = peersCol[n-1]
		_ = partitionCol[n-1]
		// Capture the slice in order for BCE to occur.
		distinctCol := p.distinctCol
		_ = distinctCol[n-1]
		for i := 0; i < n; i++ {
			//gcassert:bce
			peersCol[i] = partitionCol[i] || distinctCol[i]
		}
		// {{else}}
		// The new peer group begins when i'th tuple is different from i-1'th (in
		// which case p.distinctCol[i] is 'true'), so we simply need to copy over
		// p.distinctCol.
		copy(peersCol[:n], p.distinctCol[:n])
		// {{end}}
		// {{end}}
	}
	return b
}

// {{end}}
