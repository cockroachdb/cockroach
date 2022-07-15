// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tsearch

import (
	"math/rand"
	"strconv"
	"strings"
)

var alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// RandomTSVector returns a random TSVector for testing.
func RandomTSVector(rng *rand.Rand) TSVector {
	for {
		nTerms := 1 + rng.Intn(10)
		var b strings.Builder
		for i := 0; i < nTerms; i++ {
			l := make([]byte, 1+rng.Intn(10))
			for i := range l {
				l[i] = alphabet[rng.Intn(len(alphabet))]
			}
			b.Write(l)
			if rng.Intn(2) == 0 {
				b.WriteString(":")
				for j := 0; j < 1+rng.Intn(5); j++ {
					if j > 0 {
						b.WriteString(",")
					}
					b.WriteString(strconv.Itoa(rng.Intn(1000)))
					// Write a random "weight" from a-d.
					b.WriteByte(byte('a' + rng.Intn('d'-'a')))
				}
			}
			b.WriteByte(' ')
		}
		vec, err := ParseTSVector(b.String())
		if err != nil {
			continue
		}
		return vec
	}
}

// RandomTSQuery returns a random TSQuery for testing.
func RandomTSQuery(rng *rand.Rand) TSQuery {
	// TODO(jordan): make this a more robust random query generator
	for {
		l := make([]byte, 1+rng.Intn(10))
		for i := range l {
			l[i] = alphabet[rng.Intn(len(alphabet))]
		}
		query, err := ParseTSQuery(string(l))
		if err != nil {
			continue
		}
		return query
	}
}
