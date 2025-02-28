// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// CombineFileInfo combines the SST files and picks splits based on the key sample.
func CombineFileInfo(files []SSTFiles) ([]execinfrapb.BulkMergeSpec_SST, []roachpb.Key) {
	result := make([]execinfrapb.BulkMergeSpec_SST, 0)
	samples := make([]roachpb.Key, 0)
	for _, file := range files {
		for _, sst := range file.SST {
			result = append(result, execinfrapb.BulkMergeSpec_SST{
				StartKey: []byte(sst.StartKey),
				EndKey:   []byte(sst.EndKey),
				Uri:      sst.URI,
			})
		}
		for _, sample := range file.RowSamples {
			samples = append(samples, roachpb.Key(sample))
		}
	}
	return result, samples
}
