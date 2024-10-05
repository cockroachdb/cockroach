// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execbuilder

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestColOrdMap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxCol = 100
	m := newColOrdMap(maxCol)
	oracle := make(map[opt.ColumnID]int)
	rng, _ := randutil.NewTestRand()

	const numOps = 1000
	for i := 0; i < numOps; i++ {
		col := opt.ColumnID(rng.Intn(maxCol + 1))
		ord := rng.Intn(math.MaxInt32)

		oracle[col] = ord
		m.Set(col, ord)

		validate(t, m, oracle)

		// Periodically clear or copy the map.
		n := rng.Intn(100)
		switch {
		case n < 5:
			oracle = make(map[opt.ColumnID]int)
			m.Clear()
			validate(t, m, oracle)
		case n < 15:
			cpy := newColOrdMap(maxCol)
			cpy.CopyFrom(m)
			m = cpy
			validate(t, m, oracle)
		}
	}
}

func validate(t *testing.T, m colOrdMap, oracle map[opt.ColumnID]int) {
	maxOracleOrd := -1
	for col, oracleOrd := range oracle {
		if ord, ok := m.Get(col); !ok || ord != oracleOrd {
			t.Errorf("expected map to contain %d:%d", col, oracleOrd)
		}
		maxOracleOrd = max(maxOracleOrd, oracleOrd)
	}

	if m.MaxOrd() != maxOracleOrd {
		t.Errorf("expected max ordinal of %d, found %d", maxOracleOrd, m.MaxOrd())
	}

	m.ForEach(func(col opt.ColumnID, ord int) {
		oracleOrd, ok := oracle[col]
		if !ok || ord != oracleOrd {
			t.Errorf("unexpected col:ord in map %d:%d, oracle: %v", col, ord, oracle)
		}
	})
}

func TestMaxOrd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxCol = 100
	m := newColOrdMap(maxCol)

	assertMax := func(expectedMax int) {
		maxOrd := m.MaxOrd()
		if maxOrd != expectedMax {
			t.Errorf("expected empty map to have MaxOrd of %d, got %d", expectedMax, maxOrd)
		}
	}

	// An empty map has a max ordinal of -1.
	assertMax(-1)

	m.Set(1, 2)
	assertMax(2)

	m.Set(1, 3)
	m.Set(2, 3)
	assertMax(3)

	m.Set(1, 1)
	assertMax(3)

	m.Set(2, 1)
	assertMax(1)

	m.Set(1, 0)
	assertMax(1)
}
