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

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
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
	allocator *Allocator,
	input Operator,
	inputTyps []coltypes.T,
	orderingCols []execinfrapb.Ordering_Column,
	partitionColIdx int,
	outputColIdx int,
) (op Operator, err error) {
	allPeers := len(orderingCols) == 0
	var distinctCol []bool
	if !allPeers {
		orderIdxs := make([]uint32, len(orderingCols))
		for i, ordCol := range orderingCols {
			orderIdxs[i] = ordCol.ColIdx
		}
		input, distinctCol, err = OrderedDistinctColsToOperators(
			input, orderIdxs, inputTyps,
		)
		if err != nil {
			return nil, err
		}
	}
	initFields := windowPeerGrouperInitFields{
		OneInputNode:    NewOneInputNode(input),
		allocator:       allocator,
		partitionColIdx: partitionColIdx,
		distinctCol:     distinctCol,
		outputColIdx:    outputColIdx,
	}
	if allPeers {
		if partitionColIdx != columnOmitted {
			return &windowPeerGrouperAllPeersWithPartitionOp{
				windowPeerGrouperInitFields: initFields,
			}, nil
		}
		return &windowPeerGrouperAllPeersNoPartitionOp{
			windowPeerGrouperInitFields: initFields,
		}, nil
	}
	if partitionColIdx != columnOmitted {
		return &windowPeerGrouperWithPartitionOp{
			windowPeerGrouperInitFields: initFields,
		}, nil
	}
	return &windowPeerGrouperNoPartitionOp{
		windowPeerGrouperInitFields: initFields,
	}, nil
}

type windowPeerGrouperInitFields struct {
	OneInputNode

	allocator       *Allocator
	partitionColIdx int
	// distinctCol is the output column of the chain of ordered distinct
	// operators in which 'true' will indicate that a new peer group begins with
	// the corresponding tuple.
	distinctCol  []bool
	outputColIdx int
}

// {{range . }}

type _PEER_GROUPER_STRINGOp struct {
	windowPeerGrouperInitFields
	// {{if and .AllPeers (not .HasPartition)}}
	seenFirstTuple bool
	// {{end}}
}

var _ Operator = &_PEER_GROUPER_STRINGOp{}

func (p *_PEER_GROUPER_STRINGOp) Init() {
	p.input.Init()
}

func (p *_PEER_GROUPER_STRINGOp) Next(ctx context.Context) coldata.Batch {
	b := p.input.Next(ctx)
	n := b.Length()
	if n == 0 {
		return b
	}
	p.allocator.MaybeAddColumn(b, coltypes.Bool, p.outputColIdx)
	// {{if .HasPartition}}
	partitionCol := b.ColVec(p.partitionColIdx).Bool()
	// {{end}}
	sel := b.Selection()
	peersVec := b.ColVec(p.outputColIdx).Bool()
	if sel != nil {
		for _, i := range sel[:n] {
			// {{if .AllPeers}}
			// {{if .HasPartition}}
			// All tuples within the partition are peers, so we simply need to copy
			// over partitionCol according to sel.
			peersVec[i] = partitionCol[i]
			// {{else}}
			// There is only one partition and all tuples within it are peers, so we
			// need to set 'true' to only the first tuple ever seen.
			peersVec[i] = !p.seenFirstTuple
			p.seenFirstTuple = true
			// {{end}}
			// {{else}}
			// {{if .HasPartition}}
			// The new peer group begins either when a new partition begins (in which
			// case partitionCol[i] is 'true') or when i'th tuple is different from
			// i-1'th (in which case p.distinctCol[i] is 'true').
			peersVec[i] = partitionCol[i] || p.distinctCol[i]
			// {{else}}
			// The new peer group begins when i'th tuple is different from i-1'th (in
			// which case p.distinctCol[i] is 'true').
			peersVec[i] = p.distinctCol[i]
			// {{end}}
			// {{end}}
		}
	} else {
		// {{if .AllPeers}}
		// {{if .HasPartition}}
		// All tuples within the partition are peers, so we simply need to copy
		// over partitionCol.
		copy(peersVec[:n], partitionCol[:n])
		// {{else}}
		// There is only one partition and all tuples within it are peers, so we
		// need to set 'true' to only the first tuple ever seen.
		copy(peersVec[:n], zeroBoolColumn[:n])
		peersVec[0] = !p.seenFirstTuple
		p.seenFirstTuple = true
		// {{end}}
		// {{else}}
		// {{if .HasPartition}}
		// The new peer group begins either when a new partition begins (in which
		// case partitionCol[i] is 'true') or when i'th tuple is different from
		// i-1'th (in which case p.distinctCol[i] is 'true').
		for i := range peersVec[:n] {
			peersVec[i] = partitionCol[i] || p.distinctCol[i]
		}
		// {{else}}
		// The new peer group begins when i'th tuple is different from i-1'th (in
		// which case p.distinctCol[i] is 'true'), so we simply need to copy over
		// p.distinctCol.
		copy(peersVec[:n], p.distinctCol[:n])
		// {{end}}
		// {{end}}
	}
	return b
}

// {{end}}
