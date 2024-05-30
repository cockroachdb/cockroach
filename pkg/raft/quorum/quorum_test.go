// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quorum

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftstoreliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestComputeQSE(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int32) raftstoreliveness.StoreLivenessExpiration {
		return raftstoreliveness.StoreLivenessExpiration(hlc.Timestamp{
			Logical: ts,
		})
	}

	testCases := []struct {
		ids     []uint64
		support map[uint64]raftstoreliveness.StoreLivenessExpiration
		expQSE  raftstoreliveness.StoreLivenessExpiration
	}{
		{
			ids:     []uint64{1, 2, 3},
			support: map[uint64]raftstoreliveness.StoreLivenessExpiration{1: ts(10), 2: ts(20), 3: ts(15)},
			expQSE:  ts(15),
		},
		{
			ids:     []uint64{1, 2, 3, 4},
			support: map[uint64]raftstoreliveness.StoreLivenessExpiration{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(20)},
			expQSE:  ts(15),
		},
		{
			ids:     []uint64{1, 2, 3, 4, 5},
			support: map[uint64]raftstoreliveness.StoreLivenessExpiration{1: ts(10), 2: ts(20), 3: ts(15), 4: ts(20), 5: ts(20)},
			expQSE:  ts(20),
		},
		{
			ids:     []uint64{1, 2, 3},
			support: map[uint64]raftstoreliveness.StoreLivenessExpiration{1: ts(10), 2: ts(20)},
			expQSE:  ts(10),
		},
		{
			ids:     []uint64{1, 2, 3},
			support: map[uint64]raftstoreliveness.StoreLivenessExpiration{1: ts(10)},
			expQSE:  raftstoreliveness.MinExpiration,
		},
		{
			ids:     []uint64{},
			support: map[uint64]raftstoreliveness.StoreLivenessExpiration{},
			expQSE:  raftstoreliveness.MaxExpiration,
		},
	}

	for _, tc := range testCases {
		c := MajorityConfig{}
		for _, id := range tc.ids {
			c[id] = struct{}{}
		}

		require.Equal(t, tc.expQSE, c.ComputeQSE(tc.support))
	}
}
