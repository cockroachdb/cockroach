// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/stretchr/testify/require"
)

func TestAddIgnoredSeqNumRange(t *testing.T) {
	type r = enginepb.IgnoredSeqNumRange

	mr := func(a, b enginepb.TxnSeq) r {
		return r{Start: a, End: b}
	}

	testData := []struct {
		list     []r
		newRange r
		exp      []r
	}{
		{[]r{},
			mr(1, 2),
			[]r{mr(1, 2)}},
		{[]r{mr(1, 2)},
			mr(1, 4),
			[]r{mr(1, 4)}},
		{[]r{mr(1, 2), mr(3, 6)},
			mr(8, 10),
			[]r{mr(1, 2), mr(3, 6), mr(8, 10)}},
		{[]r{mr(1, 2), mr(5, 6)},
			mr(3, 8),
			[]r{mr(1, 2), mr(3, 8)}},
		{[]r{mr(1, 2), mr(5, 6)},
			mr(1, 8),
			[]r{mr(1, 8)}},
	}

	for _, tc := range testData {
		nl := AddIgnoredSeqNumRange(tc.list, tc.newRange)
		require.Equal(t, tc.exp, nl)
	}
}
