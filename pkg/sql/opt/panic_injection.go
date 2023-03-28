// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const panicInjectionProbability = 0.0005

// MaybeInjectOptimizerTestingPanic has a small chance of creating a panic. This
// is used to test that the optimizer panic-catching error propagation is
// correctly set up in all cases. This is only done if TestingOptimizerInjectPanics
// is enabled.
func MaybeInjectOptimizerTestingPanic(ctx context.Context, evalCtx *eval.Context) {
	if evalCtx.SessionData().TestingOptimizerInjectPanics {
		if rand.Float64() < panicInjectionProbability {
			log.Info(ctx, "injecting panic in optimizer")
			panic(errors.AssertionFailedf("injected panic in optimizer"))
		}
	}
}
