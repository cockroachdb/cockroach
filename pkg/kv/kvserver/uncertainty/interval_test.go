// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uncertainty

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestInterval_IsUncertain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeTs := func(walltime int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: walltime}
	}
	emptyTs := makeTs(0)

	testCases := []struct {
		localLim, globalLim, valueTs, localTs hlc.Timestamp
		exp                                   bool
	}{
		// With local timestamp equal to value timestamp.
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(5), localTs: makeTs(5), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(10), localTs: makeTs(10), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(15), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(20), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(25), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(5), localTs: makeTs(5), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(10), localTs: makeTs(10), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(15), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(20), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(25), exp: false},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(5), localTs: makeTs(5), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(10), localTs: makeTs(10), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(15), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(20), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(25), exp: false},
		// With local timestamp below value timestamp.
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(10), localTs: makeTs(5), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(5), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(10), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(5), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(10), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(15), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(5), exp: false},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(10), exp: false},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(15), exp: false},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(20), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(10), localTs: makeTs(5), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(5), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(10), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(5), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(10), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(15), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(5), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(10), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(15), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(20), exp: false},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(10), localTs: makeTs(5), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(5), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(15), localTs: makeTs(10), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(5), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(10), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(20), localTs: makeTs(15), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(5), exp: false},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(10), exp: false},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(15), exp: false},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(25), localTs: makeTs(20), exp: false},
		// Empty uncertainty intervals.
		{localLim: emptyTs, globalLim: emptyTs, valueTs: makeTs(10), localTs: makeTs(10), exp: false},
		{localLim: emptyTs, globalLim: emptyTs, valueTs: makeTs(10), localTs: makeTs(5), exp: false},
	}
	for _, test := range testCases {
		in := Interval{GlobalLimit: test.globalLim, LocalLimit: hlc.ClockTimestamp(test.localLim)}
		res := in.IsUncertain(test.valueTs, hlc.ClockTimestamp(test.localTs))
		require.Equal(t, test.exp, res, "%+v", test)
	}
}
