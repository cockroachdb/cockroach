// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// TODO(yuzefovich): add actual implementation.

// newExternalSorter returns a disk-backed general sort operator.
func newExternalSorter(
	allocator *Allocator,
	input Operator,
	inputTypes []coltypes.T,
	orderingCols []execinfrapb.Ordering_Column,
) Operator {
	inMemSorter, err := newSorter(
		allocator, newAllSpooler(allocator, input, inputTypes),
		inputTypes, orderingCols,
	)
	if err != nil {
		execerror.VectorizedInternalPanic(err)
	}
	return &externalSorter{OneInputNode: NewOneInputNode(inMemSorter)}
}

type externalSorter struct {
	OneInputNode
}

var _ Operator = &externalSorter{}

func (s *externalSorter) Init() {
	s.input.Init()
}

func (s *externalSorter) Next(ctx context.Context) coldata.Batch {
	return s.input.Next(ctx)
}
