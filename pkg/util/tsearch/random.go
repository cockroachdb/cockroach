// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	// TODO(jordan): add parenthesis grouping to the random query generator
	nTerms := 1 + rng.Intn(5)
	for {
		var sb strings.Builder
		for i := 0; i < nTerms; i++ {
			l := make([]byte, 1+rng.Intn(10))
			for i := range l {
				l[i] = alphabet[rng.Intn(len(alphabet))]
			}
			if rng.Intn(4) == 0 {
				// Make it a negation query!
				sb.WriteString("!")
			}
			sb.Write(l)
			sb.WriteString(" ")
			if i < nTerms-1 {
				infixOp := rng.Intn(3)
				var opstr string
				switch infixOp {
				case 0:
					opstr = "&"
				case 1:
					opstr = "|"
				case 2:
					opstr = "<->"
				}
				sb.WriteString(opstr)
				sb.WriteString(" ")
			}
		}

		query, err := ParseTSQuery(sb.String())
		if err != nil {
			continue
		}
		return query
	}
}
