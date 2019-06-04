// Copyright 2019 The Cockroach Authors.
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

// fnOp is an operator that executes an arbitrary function for its side-effects,
// once per input batch, passing the input batch unmodified along.
type fnOp struct {
	input Operator

	fn func()
}

func (f fnOp) Init() {
	f.input.Init()
}

func (f fnOp) Next(ctx context.Context) coldata.Batch {
	batch := f.input.Next(ctx)
	f.fn()
	return batch
}
