// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enginepb

import (
	fmt "fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataGetPrevIntentSeq(t *testing.T) {
	for _, tc := range []struct {
		history      []TxnSeq
		search       TxnSeq
		expPrevSeq   TxnSeq
		expPrevFound bool
	}{
		{[]TxnSeq{}, 0, 0, false},
		{[]TxnSeq{}, 1, 0, false},
		{[]TxnSeq{}, 2, 0, false},
		{[]TxnSeq{}, 3, 0, false},
		{[]TxnSeq{0}, 0, 0, false},
		{[]TxnSeq{0}, 1, 0, true},
		{[]TxnSeq{0}, 2, 0, true},
		{[]TxnSeq{0}, 3, 0, true},
		{[]TxnSeq{1}, 0, 0, false},
		{[]TxnSeq{1}, 1, 0, false},
		{[]TxnSeq{1}, 2, 1, true},
		{[]TxnSeq{1}, 3, 1, true},
		{[]TxnSeq{0, 1}, 0, 0, false},
		{[]TxnSeq{0, 1}, 1, 0, true},
		{[]TxnSeq{0, 1}, 2, 1, true},
		{[]TxnSeq{0, 1}, 3, 1, true},
		{[]TxnSeq{0, 2}, 0, 0, false},
		{[]TxnSeq{0, 2}, 1, 0, true},
		{[]TxnSeq{0, 2}, 2, 0, true},
		{[]TxnSeq{0, 2}, 3, 2, true},
		{[]TxnSeq{1, 2}, 0, 0, false},
		{[]TxnSeq{1, 2}, 1, 0, false},
		{[]TxnSeq{1, 2}, 2, 1, true},
		{[]TxnSeq{1, 2}, 3, 2, true},
		{[]TxnSeq{0, 1, 2}, 0, 0, false},
		{[]TxnSeq{0, 1, 2}, 1, 0, true},
		{[]TxnSeq{0, 1, 2}, 2, 1, true},
		{[]TxnSeq{0, 1, 2}, 3, 2, true},
	} {
		name := fmt.Sprintf("%v/%d", tc.history, tc.search)
		t.Run(name, func(t *testing.T) {
			var meta MVCCMetadata
			for _, seq := range tc.history {
				meta.IntentHistory = append(meta.IntentHistory, MVCCMetadata_SequencedIntent{
					Sequence: seq,
				})
			}

			prevSeq, prevFound := meta.GetPrevIntentSeq(tc.search)
			require.Equal(t, tc.expPrevSeq, prevSeq)
			require.Equal(t, tc.expPrevFound, prevFound)
		})
	}
}
