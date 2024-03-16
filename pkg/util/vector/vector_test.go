package vector

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
)

func TestRoundtripRandomPGVector(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 1000; i++ {
		v := Random(rng)
		encoded, err := Encode(nil, v)
		assert.NoError(t, err)
		roundtripped, err := Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, v.String(), roundtripped.String())
		reEncoded, err := Encode(nil, roundtripped)
		assert.NoError(t, err)
		assert.Equal(t, encoded, reEncoded)
	}
}
