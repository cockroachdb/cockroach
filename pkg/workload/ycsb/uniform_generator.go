package ycsb

import (
	"github.com/backport/pkg/util/syncutil"
	"math/rand"
)

// UniformGenerator is a random number generator that generates draws from a
// uniform distribution.
type UniformGenerator struct {
	// The underlying RNG
	uniformGenMu UniformGeneratorMU
	minValue     uint64
}

// UniformGeneratorMU holds variables which must be globally synced.
type UniformGeneratorMU struct {
	mu       syncutil.Mutex
	r        *rand.Rand
	sequence uint64
}

// NewUniformGenerator constructs a new UniformGenerator with the given parameters.
// It returns an error if the parameters are outside the accepted range.
func NewUniformGenerator(rng *rand.Rand, minValue uint64) (*UniformGenerator, error) {

	z := UniformGenerator{
		minValue: minValue,
		uniformGenMu: UniformGeneratorMU{
			r:        rng,
			sequence: minValue,
		},
	}

	return &z, nil
}

// IMaxHead returns the current value of IMaxHead, without incrementing.
func (z *UniformGenerator) IMaxHead() uint64 {
	z.uniformGenMu.mu.Lock()
	max := z.uniformGenMu.sequence
	z.uniformGenMu.mu.Unlock()
	return max
}

// IncrementIMax increments the sequence number.
func (z *UniformGenerator) IncrementIMax() error {
	z.uniformGenMu.mu.Lock()
	z.uniformGenMu.sequence += 1
	z.uniformGenMu.mu.Unlock()
	return nil
}

// Uint64 returns a random Uint64 between min and sequence, drawn from a uniform
// distribution.
func (z *UniformGenerator) Uint64() uint64 {
	z.uniformGenMu.mu.Lock()
	result := (rand.Uint64() % (z.uniformGenMu.sequence - z.minValue)) + z.minValue
	z.uniformGenMu.mu.Unlock()
	return uint64(result)
}
