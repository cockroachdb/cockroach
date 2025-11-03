// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRowSampleReservoirCapsSamples(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var base fileAllocatorBase
	expected := make(map[string]struct{})
	total := maxRowSamples*2 + 17
	for i := 0; i < total; i++ {
		key := roachpb.Key(fmt.Sprintf("key-%04d", i))
		expected[string(key)] = struct{}{}
		span := roachpb.Span{Key: key, EndKey: key.Next()}
		base.addFile(fmt.Sprintf("uri-%d", i), span, key, 1)
	}

	require.Equal(t, total, base.totalRowSamples)
	require.Len(t, base.fileInfo.RowSamples, maxRowSamples)

	for _, sample := range base.fileInfo.RowSamples {
		_, ok := expected[sample]
		require.True(t, ok, "sample %q must correspond to ingested keys", sample)
	}
}

func TestRowSampleSkipsEmptyKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var base fileAllocatorBase
	key := roachpb.Key("some-key")
	span := roachpb.Span{Key: key, EndKey: key.Next()}

	base.addFile("uri-1", span, key, 1)
	base.addFile("uri-2", span, nil, 1)

	require.Len(t, base.fileInfo.RowSamples, 1)
	require.Equal(t, string(key), base.fileInfo.RowSamples[0])
}
