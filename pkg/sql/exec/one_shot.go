// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package exec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
)

// oneShotOp is an operator that does an arbitrary operation on the first batch
// that it gets, then deletes itself from the operator tree. This is useful for
// first-Next initialization that has to happen in an operator.
type oneShotOp struct {
	input Operator

	outputSourceRef *Operator

	fn func(batch coldata.Batch)
}

var _ Operator = &oneShotOp{}

func (o *oneShotOp) Init() {
	o.input.Init()
}

func (o *oneShotOp) Next(ctx context.Context) coldata.Batch {
	batch := o.input.Next(ctx)

	// Do our one-time work.
	o.fn(batch)
	// Swap out our output's input with our input, so we don't have to get called
	// anymore.
	*o.outputSourceRef = o.input

	return batch
}
