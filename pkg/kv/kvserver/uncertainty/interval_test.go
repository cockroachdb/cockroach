// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	makeSynTs := func(walltime int64) hlc.Timestamp {
		return makeTs(walltime).WithSynthetic(true)
	}
	emptyTs := makeTs(0)

	testCases := []struct {
		localLim, globalLim, valueTs hlc.Timestamp
		exp                          bool
	}{
		// Without synthetic value.
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(5), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(10), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(15), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(20), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeTs(25), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(5), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(10), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(15), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(20), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeTs(25), exp: false},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(5), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(10), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(15), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(20), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeTs(25), exp: false},
		// With synthetic value.
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeSynTs(5), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeSynTs(10), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeSynTs(15), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeSynTs(20), exp: true},
		{localLim: emptyTs, globalLim: makeTs(20), valueTs: makeSynTs(25), exp: false},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeSynTs(5), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeSynTs(10), exp: true},
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeSynTs(15), exp: true}, // different
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeSynTs(20), exp: true}, // different
		{localLim: makeTs(10), globalLim: makeTs(20), valueTs: makeSynTs(25), exp: false},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeSynTs(5), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeSynTs(10), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeSynTs(15), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeSynTs(20), exp: true},
		{localLim: makeTs(20), globalLim: makeTs(20), valueTs: makeSynTs(25), exp: false},
		// Empty uncertainty intervals.
		{localLim: emptyTs, globalLim: emptyTs, valueTs: makeTs(5), exp: false},
		{localLim: emptyTs, globalLim: emptyTs, valueTs: makeSynTs(5), exp: false},
	}
	for _, test := range testCases {
		in := Interval{GlobalLimit: test.globalLim, LocalLimit: hlc.ClockTimestamp(test.localLim)}
		require.Equal(t, test.exp, in.IsUncertain(test.valueTs), "%+v", test)
	}
}
