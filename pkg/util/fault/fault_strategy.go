// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fault

import (
	"context"
	"math"
	"math/rand/v2"
	"sync/atomic"
)

// Strategy is an interface that is used to control fault injection in a
// component that has instrumented fault injection.
type Strategy interface {
	// ShouldInject returns true if the operation should return a non-recoverable
	// error.
	ShouldInject(ctx context.Context, faultName string) bool
}

// NewProbabilisticFaults creates a strategy that fails every fault with a
// fixed probability.
func NewProbabilisticFaults(probability float64) *ProbabilisticFaults {
	p := &ProbabilisticFaults{}
	p.SetProbability(probability)
	return p
}

type ProbabilisticFaults struct {
	probability atomic.Uint64
}

func (p *ProbabilisticFaults) SetProbability(probability float64) {
	p.probability.Store(math.Float64bits(probability))
}

func (p *ProbabilisticFaults) ShouldInject(ctx context.Context, faultName string) bool {
	probability := math.Float64frombits(p.probability.Load())
	return rand.Float64() < probability
}

type nopStrategy struct {
}

func (s *nopStrategy) ShouldInject(ctx context.Context, faultName string) bool {
	return false
}

// NopStrategy is a fault injection strategy that never injects faults.
var NopStrategy Strategy = &nopStrategy{}
