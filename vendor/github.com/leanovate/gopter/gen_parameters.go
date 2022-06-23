package gopter

import (
	"math/rand"
	"time"
)

// GenParameters encapsulates the parameters for all generators.
type GenParameters struct {
	MinSize        int
	MaxSize        int
	MaxShrinkCount int
	Rng            *rand.Rand
}

// WithSize modifies the size parameter. The size parameter defines an upper bound for the size of
// generated slices or strings.
func (p *GenParameters) WithSize(size int) *GenParameters {
	newParameters := *p
	newParameters.MaxSize = size
	return &newParameters
}

// NextBool create a random boolean using the underlying Rng.
func (p *GenParameters) NextBool() bool {
	return p.Rng.Int63()&1 == 0
}

// NextInt64 create a random int64 using the underlying Rng.
func (p *GenParameters) NextInt64() int64 {
	v := p.Rng.Int63()
	if p.NextBool() {
		return -v
	}
	return v
}

// NextUint64 create a random uint64 using the underlying Rng.
func (p *GenParameters) NextUint64() uint64 {
	first := uint64(p.Rng.Int63())
	second := uint64(p.Rng.Int63())

	return (first << 1) ^ second
}

// CloneWithSeed clone the current parameters with a new seed.
// This is useful to create subsections that can rerun (provided you keep the
// seed)
func (p *GenParameters) CloneWithSeed(seed int64) *GenParameters {
	return &GenParameters{
		MinSize:        p.MinSize,
		MaxSize:        p.MaxSize,
		MaxShrinkCount: p.MaxShrinkCount,
		Rng:            rand.New(NewLockedSource(seed)),
	}
}

// DefaultGenParameters creates default GenParameters.
func DefaultGenParameters() *GenParameters {
	seed := time.Now().UnixNano()

	return &GenParameters{
		MinSize:        0,
		MaxSize:        100,
		MaxShrinkCount: 1000,
		Rng:            rand.New(NewLockedSource(seed)),
	}
}

// MinGenParameters creates minimal GenParameters.
// Note: Most likely you do not want to use these for actual testing
func MinGenParameters() *GenParameters {
	seed := time.Now().UnixNano()

	return &GenParameters{
		MinSize:        0,
		MaxSize:        0,
		MaxShrinkCount: 0,
		Rng:            rand.New(NewLockedSource(seed)),
	}
}
