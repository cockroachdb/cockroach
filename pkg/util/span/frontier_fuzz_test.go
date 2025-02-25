// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package span

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func fuzzFrontier(f *testing.F) {
	seed := randutil.NewPseudoSeed()
	rnd := rand.New(rand.NewSource(seed))

	spanMaker, initialSpan := newSpanMaker(6, rnd)
	const corpusSize = 2 << 10
	for i := 0; i < corpusSize; i++ {
		s := spanMaker.rndSpan()
		// Add fuzz corpus.  Note: timestamps added could be negative, which
		// of course is not a valid timestamp, but makes it so much fun to test.
		f.Add([]byte(s.Key), []byte(s.EndKey), rnd.Intn(corpusSize)-rnd.Intn(corpusSize))
	}

	mkFrontier := func() Frontier {
		sf, err := MakeFrontier(initialSpan)
		if err != nil {
			f.Fatal(err)
		}
		return sf
	}

	sf := &captureHistoryFrontier{SpanFrontier: mkFrontier()}

	f.Fuzz(func(t *testing.T, startKey, endKey []byte, walltime int) {
		// NB: copy start and end keys: fuzzer mutates inputs.
		var sp roachpb.Span
		sp.Key = append(sp.Key, startKey...)
		sp.EndKey = append(sp.EndKey, endKey...)

		if err := forwardWithErrorCheck(sf, sp, int64(walltime)); err != nil {
			t.Fatalf("err=%+v f=%s History:\n%s", err, sf, sf.History())
		}

		startKey, endKey, err := checkContiguousFrontier(sf)
		if err != nil {
			t.Fatalf("err=%s\nHistory:\n%s", err, sf.History())
		}
		// At the end of iteration, we should have record start/end key equal to the initial span.
		if !initialSpan.Key.Equal(startKey) || !initialSpan.EndKey.Equal(endKey) {
			t.Fatalf("expected to see entire %s sf, saw [%s-%s)", initialSpan, startKey, endKey)
		}
	})
}

func FuzzBtreeFrontier(f *testing.F) {
	fuzzFrontier(f)
}
