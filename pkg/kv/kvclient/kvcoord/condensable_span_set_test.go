// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Test that the size of the condensableSpanSet is properly maintained when
// contiguous spans are merged.
func TestCondensableSpanSetMergeContiguousSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := condensableSpanSet{}
	s.insert(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")})
	s.insert(roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")})
	require.Equal(t, int64(4), s.bytes)
	s.mergeAndSort()
	require.Equal(t, int64(2), s.bytes)
}
