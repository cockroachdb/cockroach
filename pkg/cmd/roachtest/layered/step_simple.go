// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package layered

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
)

type SimpleStep struct {
	AtLeastV21Dot2MixedSupporter
	N string
	O registry.Owner
	R bool
	F func(ctx context.Context, r *rand.Rand, t Fataler, c SeqEnv)
}

func (s *SimpleStep) String() string {
	return s.N
}

func (s *SimpleStep) Owner() registry.Owner {
	return s.O
}

func (s *SimpleStep) Idempotent() bool {
	return s.R
}

func (s *SimpleStep) Run(ctx context.Context, r *rand.Rand, t Fataler, c SeqEnv) {
	s.F(ctx, r, t, c)
}
