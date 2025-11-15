// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCombineFileInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	span := func(key, end string) roachpb.Span {
		return roachpb.Span{
			Key:    []byte(key),
			EndKey: []byte(end),
		}
	}

	sst := func(uri, start, end string) *SSTFileInfo {
		return &SSTFileInfo{
			URI:      uri,
			StartKey: roachpb.Key(start),
			EndKey:   roachpb.Key(end),
			FileSize: 1024,
		}
	}

	tests := []struct {
		name              string
		files             []SSTFiles
		schemaSpans       []roachpb.Span
		expectedSSTs      []execinfrapb.BulkMergeSpec_SST
		expectedMergeSpan []roachpb.Span
	}{
		{
			name: "empty samples",
			files: []SSTFiles{
				{
					SST:        []*SSTFileInfo{sst("file1.sst", "b", "d")},
					RowSamples: []string{},
					TotalSize:  1024,
				},
			},
			schemaSpans: []roachpb.Span{span("a", "z")},
			expectedSSTs: []execinfrapb.BulkMergeSpec_SST{
				{URI: "file1.sst", StartKey: "b", EndKey: "d"},
			},
			expectedMergeSpan: []roachpb.Span{span("a", "z")},
		},
		{
			name: "single schema span with samples",
			files: []SSTFiles{
				{
					SST:        []*SSTFileInfo{sst("file1.sst", "a", "e")},
					RowSamples: []string{"c", "f"},
					TotalSize:  1024,
				},
				{
					SST:        []*SSTFileInfo{sst("file2.sst", "e", "z")},
					RowSamples: []string{"h"},
					TotalSize:  1024,
				},
			},
			schemaSpans: []roachpb.Span{span("a", "z")},
			expectedSSTs: []execinfrapb.BulkMergeSpec_SST{
				{URI: "file1.sst", StartKey: "a", EndKey: "e"},
				{URI: "file2.sst", StartKey: "e", EndKey: "z"},
			},
			expectedMergeSpan: []roachpb.Span{
				span("a", "c"),
				span("c", "f"),
				span("f", "h"),
				span("h", "z"),
			},
		},
		{
			name: "multiple schema spans",
			files: []SSTFiles{
				{
					SST:        []*SSTFileInfo{sst("file1.sst", "a", "m")},
					RowSamples: []string{"c", "f"},
					TotalSize:  1024,
				},
				{
					SST:        []*SSTFileInfo{sst("file2.sst", "m", "z")},
					RowSamples: []string{"n", "r"},
					TotalSize:  1024,
				},
			},
			schemaSpans: []roachpb.Span{
				span("a", "m"),
				span("m", "z"),
			},
			expectedSSTs: []execinfrapb.BulkMergeSpec_SST{
				{URI: "file1.sst", StartKey: "a", EndKey: "m"},
				{URI: "file2.sst", StartKey: "m", EndKey: "z"},
			},
			expectedMergeSpan: []roachpb.Span{
				span("a", "c"),
				span("c", "f"),
				span("f", "m"),
				span("m", "n"),
				span("n", "r"),
				span("r", "z"),
			},
		},
		{
			name: "unsorted samples across files",
			files: []SSTFiles{
				{
					SST:        []*SSTFileInfo{sst("file1.sst", "a", "m")},
					RowSamples: []string{"f", "c"}, // Unsorted
					TotalSize:  1024,
				},
				{
					SST:        []*SSTFileInfo{sst("file2.sst", "m", "z")},
					RowSamples: []string{"r", "n"}, // Unsorted
					TotalSize:  1024,
				},
			},
			schemaSpans: []roachpb.Span{
				span("a", "m"),
				span("m", "z"),
			},
			expectedSSTs: []execinfrapb.BulkMergeSpec_SST{
				{URI: "file1.sst", StartKey: "a", EndKey: "m"},
				{URI: "file2.sst", StartKey: "m", EndKey: "z"},
			},
			expectedMergeSpan: []roachpb.Span{
				span("a", "c"),
				span("c", "f"),
				span("f", "m"),
				span("m", "n"),
				span("n", "r"),
				span("r", "z"),
			},
		},
		{
			name: "multiple SSTs per file",
			files: []SSTFiles{
				{
					SST: []*SSTFileInfo{
						sst("file1a.sst", "a", "e"),
						sst("file1b.sst", "e", "m"),
					},
					RowSamples: []string{"c", "g"},
					TotalSize:  2048,
				},
			},
			schemaSpans: []roachpb.Span{span("a", "z")},
			expectedSSTs: []execinfrapb.BulkMergeSpec_SST{
				{URI: "file1a.sst", StartKey: "a", EndKey: "e"},
				{URI: "file1b.sst", StartKey: "e", EndKey: "m"},
			},
			expectedMergeSpan: []roachpb.Span{
				span("a", "c"),
				span("c", "g"),
				span("g", "z"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ssts, mergeSpans, err := CombineFileInfo(tc.files, tc.schemaSpans)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSSTs, ssts)
			// Validate all returned spans are valid (Key < EndKey)
			for i, s := range mergeSpans {
				require.True(t, s.Valid(), "span %d [%q, %q) is invalid: Key >= EndKey", i, s.Key, s.EndKey)
			}
			require.Equal(t, tc.expectedMergeSpan, mergeSpans)
		})
	}
}

// TestCombineFileInfo_Validation tests that samples outside schema spans are detected.
func TestCombineFileInfo_Validation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	span := func(key, end string) roachpb.Span {
		return roachpb.Span{
			Key:    []byte(key),
			EndKey: []byte(end),
		}
	}

	files := func(samples ...string) []SSTFiles {
		return []SSTFiles{
			{
				SST:        []*SSTFileInfo{},
				RowSamples: samples,
				TotalSize:  0,
			},
		}
	}

	tests := []struct {
		name              string
		files             []SSTFiles
		schemaSpans       []roachpb.Span
		expectError       bool
		errorContains     []string
		validateNonNilRes bool
		expectedMergeSpan []roachpb.Span
	}{
		{
			name:        "sample before first span",
			files:       files("a", "c"),
			schemaSpans: []roachpb.Span{span("b", "z")},
			expectError: true,
			errorContains: []string{
				"is before schema span",
				"\"a\"",
			},
		},
		{
			name:        "sample after last span",
			files:       files("c", "z"),
			schemaSpans: []roachpb.Span{span("a", "m")},
			expectError: true,
			errorContains: []string{
				"samples outside schema spans",
				"\"z\"",
			},
		},
		{
			name:  "sample in gap between spans",
			files: files("c", "m", "r"),
			schemaSpans: []roachpb.Span{
				span("a", "f"),
				span("p", "z"),
			},
			expectError: true,
			errorContains: []string{
				"is before schema span",
				"\"m\"",
			},
		},
		{
			name:  "all samples within spans",
			files: files("c", "m", "r"),
			schemaSpans: []roachpb.Span{
				span("a", "f"),
				span("f", "z"),
			},
			expectError:       false,
			validateNonNilRes: true,
		},
		{
			name:  "samples at span boundaries",
			files: files("m"),
			schemaSpans: []roachpb.Span{
				span("a", "m"),
				span("m", "z"),
			},
			expectError:       false,
			validateNonNilRes: true,
		},
		{
			name:  "unordered spans",
			files: files("c", "p"),
			schemaSpans: []roachpb.Span{
				span("m", "z"),
				span("a", "m"), // Out of order!
			},
			expectError: true,
			errorContains: []string{
				"schema spans not ordered",
			},
		},
		{
			name:  "overlapping spans",
			files: files("c", "p"),
			schemaSpans: []roachpb.Span{
				span("a", "m"),
				span("k", "z"), // Overlaps with previous span
			},
			expectError: true,
			errorContains: []string{
				"schema spans overlapping",
			},
		},
		{
			name:        "duplicate samples",
			files:       files("c", "c", "f"),
			schemaSpans: []roachpb.Span{span("a", "z")},
			expectError: false,
			expectedMergeSpan: []roachpb.Span{
				span("a", "c"),
				span("c", "f"),
				span("f", "z"),
			},
		},
		{
			name:        "sample at schema span start",
			files:       files("a", "f"),
			schemaSpans: []roachpb.Span{span("a", "z")},
			expectError: false,
			expectedMergeSpan: []roachpb.Span{
				span("a", "f"),
				span("f", "z"),
			},
		},
		{
			name:        "multiple consecutive identical samples",
			files:       files("c", "c", "c", "f", "f"),
			schemaSpans: []roachpb.Span{span("a", "z")},
			expectError: false,
			expectedMergeSpan: []roachpb.Span{
				span("a", "c"),
				span("c", "f"),
				span("f", "z"),
			},
		},
		{
			name:  "sample at boundary between spans",
			files: files("m", "m", "p"),
			schemaSpans: []roachpb.Span{
				span("a", "m"),
				span("m", "z"),
			},
			expectError: false,
			expectedMergeSpan: []roachpb.Span{
				span("a", "m"),
				span("m", "p"),
				span("p", "z"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, result, err := CombineFileInfo(tc.files, tc.schemaSpans)
			if tc.expectError {
				require.Error(t, err)
				for _, contains := range tc.errorContains {
					require.Contains(t, err.Error(), contains)
				}
			} else {
				require.NoError(t, err)
				if tc.validateNonNilRes {
					require.NotNil(t, result)
				}
				// Validate all returned spans are valid (Key < EndKey)
				for i, s := range result {
					require.True(t, s.Valid(), "span %d [%q, %q) is invalid: Key >= EndKey", i, s.Key, s.EndKey)
				}
				if tc.expectedMergeSpan != nil {
					require.Equal(t, tc.expectedMergeSpan, result)
				}
			}
		})
	}
}
