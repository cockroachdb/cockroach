// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSSTFilesAppend(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create first SSTFiles
	sst1 := &SSTFiles{
		SST: []*SSTFileInfo{
			{
				URI:      "file1.sst",
				StartKey: roachpb.Key("a"),
				EndKey:   roachpb.Key("b"),
				FileSize: 100,
			},
			{
				URI:      "file2.sst",
				StartKey: roachpb.Key("b"),
				EndKey:   roachpb.Key("c"),
				FileSize: 200,
			},
		},
		RowSamples: []string{"sample1", "sample2"},
		TotalSize:  300,
	}

	// Create second SSTFiles
	sst2 := &SSTFiles{
		SST: []*SSTFileInfo{
			{
				URI:      "file3.sst",
				StartKey: roachpb.Key("c"),
				EndKey:   roachpb.Key("d"),
				FileSize: 150,
			},
		},
		RowSamples: []string{"sample3"},
		TotalSize:  150,
	}

	// Append sst2 to sst1
	sst1.Append(sst2)

	// Verify the results
	require.Len(t, sst1.SST, 3, "expected 3 SST files after append")
	require.Equal(t, "file1.sst", sst1.SST[0].URI)
	require.Equal(t, "file2.sst", sst1.SST[1].URI)
	require.Equal(t, "file3.sst", sst1.SST[2].URI)

	require.Len(t, sst1.RowSamples, 3, "expected 3 row samples after append")
	require.Equal(t, "sample1", sst1.RowSamples[0])
	require.Equal(t, "sample2", sst1.RowSamples[1])
	require.Equal(t, "sample3", sst1.RowSamples[2])

	require.Equal(t, uint64(450), sst1.TotalSize, "expected total size to be sum of both")
}

func TestSSTFilesAppendEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create non-empty SSTFiles
	sst1 := &SSTFiles{
		SST: []*SSTFileInfo{
			{
				URI:      "file1.sst",
				StartKey: roachpb.Key("a"),
				EndKey:   roachpb.Key("b"),
				FileSize: 100,
			},
		},
		RowSamples: []string{"sample1"},
		TotalSize:  100,
	}

	// Append empty SSTFiles
	sst2 := &SSTFiles{}
	sst1.Append(sst2)

	// Verify nothing changed (except potentially slice capacity)
	require.Len(t, sst1.SST, 1)
	require.Len(t, sst1.RowSamples, 1)
	require.Equal(t, uint64(100), sst1.TotalSize)
}

func TestSSTFilesAppendToEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Create empty SSTFiles
	sst1 := &SSTFiles{}

	// Append non-empty SSTFiles
	sst2 := &SSTFiles{
		SST: []*SSTFileInfo{
			{
				URI:      "file1.sst",
				StartKey: roachpb.Key("a"),
				EndKey:   roachpb.Key("b"),
				FileSize: 100,
			},
		},
		RowSamples: []string{"sample1"},
		TotalSize:  100,
	}

	sst1.Append(sst2)

	// Verify sst1 now contains sst2's data
	require.Len(t, sst1.SST, 1)
	require.Equal(t, "file1.sst", sst1.SST[0].URI)
	require.Len(t, sst1.RowSamples, 1)
	require.Equal(t, "sample1", sst1.RowSamples[0])
	require.Equal(t, uint64(100), sst1.TotalSize)
}
