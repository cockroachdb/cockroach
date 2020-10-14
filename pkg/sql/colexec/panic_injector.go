// Copyright 2020 The Cockroach Authors.
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
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

// panicInjector is a helper Operator that will randomly inject panics into
// Init and Next methods of the wrapped operator.
type panicInjector struct {
	OneInputNode
	rng *rand.Rand
}

var _ colexecbase.Operator = panicInjector{}

const (
	// These constants were chosen arbitrarily with the guiding thought that
	// Init() methods are called less frequently, so the probability of
	// injecting in Init() should be higher. At the same time, we don't want
	// for the vectorized flows to always run into these injected panics, so
	// both numbers are relatively low.
	initPanicInjectionProbability = 0.001
	nextPanicInjectionProbability = 0.00001
)

// NewPanicInjector creates a new panicInjector.
func NewPanicInjector(input colexecbase.Operator) colexecbase.Operator {
	rng, _ := randutil.NewPseudoRand()
	return &panicInjector{
		OneInputNode: OneInputNode{input: input},
		rng:          rng,
	}
}

func (i panicInjector) Init() {
	if i.rng.Float64() < initPanicInjectionProbability {
		colexecerror.ExpectedError(errors.New("injected panic in Init"))
	}
	i.input.Init()
}

func (i panicInjector) Next(ctx context.Context) coldata.Batch {
	if i.rng.Float64() < nextPanicInjectionProbability {
		colexecerror.ExpectedError(errors.New("injected panic in Next"))
	}
	return i.input.Next(ctx)
}
