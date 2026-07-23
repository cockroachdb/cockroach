// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package covering

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkOverlapCoveringMerge(b *testing.B) {
	var benchmark []struct {
		name   string
		inputs []Covering
	}
	//lint:ignore SA1019 deprecated
	rand.Seed(0)

	for _, numLayers := range []int{
		1,      // single backup
		24,     // hourly backups
		24 * 7, // hourly backups for a week.
	} {
		// number of elements per each backup instance
		for _, elementsPerLayer := range []int{100, 1000, 10000} {
			var inputs []Covering

			for i := 0; i < numLayers; i++ {
				var payload int
				var c Covering
				step := 1 + rand.Intn(10)

				for j := 0; j < elementsPerLayer; j += step {
					start := make([]byte, 4)
					binary.LittleEndian.PutUint32(start, uint32(j))

					end := make([]byte, 4)
					binary.LittleEndian.PutUint32(end, uint32(j+step))

					c = append(c, Range{
						Start:   start,
						End:     end,
						Payload: payload,
					})
					payload++
				}
				inputs = append(inputs, c)
			}

			benchmark = append(benchmark, struct {
				name   string
				inputs []Covering
			}{name: fmt.Sprintf("layers=%d,elems=%d", numLayers, elementsPerLayer), inputs: inputs})
		}
	}

	b.ResetTimer()
	for _, bench := range benchmark {
		inputs := bench.inputs
		b.Run(bench.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				require.NotEmpty(b, OverlapCoveringMerge(inputs))
			}
		})
	}
}
