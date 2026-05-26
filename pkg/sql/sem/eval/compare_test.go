// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/stretchr/testify/assert"
)

// Test that we can compare random datums with a nil eval context.
func TestNilEvalContextCompareRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var cmpCtx *eval.Context
	rng, _ := randutil.NewTestRand()

	for trial := 0; trial < 40; trial++ {
		typ := randgen.RandType(rng)
		left := randgen.RandDatum(rng, typ, true /* nullOk */)
		right := randgen.RandDatum(rng, typ, true /* nullOk */)
		_, err := left.Compare(ctx, cmpCtx, right)
		assert.NoError(t, err)

		// Also test comparing a random datum to itself with a nil eval context.
		var res int
		res, err = left.Compare(ctx, cmpCtx, left)
		assert.NoError(t, err)
		assert.Equal(t, 0, res)
	}
}

// Test that we can compare two equal TimeTZ datums with a nil eval context.
func TestNilEvalContextCompareTimeTZ(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var cmpCtx *eval.Context

	time := timeofday.New(22, 0, 0, 0)
	res, err := tree.NewDTimeTZFromOffset(time, 0).Compare(
		ctx,
		cmpCtx,
		tree.NewDTimeTZFromOffset(time, 0),
	)
	assert.NoError(t, err)
	assert.Equal(t, 0, res)
}
