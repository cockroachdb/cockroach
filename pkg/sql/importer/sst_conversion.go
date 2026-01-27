// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
)

// sstFilesToManifests converts bulksst.SSTFiles to jobspb.IndexBackfillSSTManifests
// for persistence. This conversion is needed because we store SST metadata in job
// progress using jobspb types to avoid circular dependencies (jobspb → bulksst → ... → jobspb).
// We reuse IndexBackfillSSTManifest (from index backfill) instead of defining
// import-specific types to avoid duplication.
func sstFilesToManifests(files *bulksst.SSTFiles) []jobspb.IndexBackfillSSTManifest {
	if files == nil {
		return nil
	}

	result := make([]jobspb.IndexBackfillSSTManifest, 0, len(files.SST))
	for _, sst := range files.SST {
		result = append(result, jobspb.IndexBackfillSSTManifest{
			URI: sst.URI,
			Span: &roachpb.Span{
				Key:    append(roachpb.Key(nil), sst.StartKey...),
				EndKey: append(roachpb.Key(nil), sst.EndKey...),
			},
			FileSize:  sst.FileSize,
			RowSample: append(roachpb.Key(nil), sst.RowSample...),
			KeyCount:  sst.KeyCount,
			// WriteTimestamp is left as zero-value since import doesn't use it
		})
	}

	return result
}

// manifestsToSSTFiles converts jobspb.IndexBackfillSSTManifests back to bulksst.SSTFiles.
// This is used when restoring SST metadata from job progress on retry.
func manifestsToSSTFiles(manifests []jobspb.IndexBackfillSSTManifest) bulksst.SSTFiles {
	result := bulksst.SSTFiles{
		SST: make([]*bulksst.SSTFileInfo, 0, len(manifests)),
	}

	for _, manifest := range manifests {
		result.SST = append(result.SST, &bulksst.SSTFileInfo{
			URI:       manifest.URI,
			StartKey:  append(roachpb.Key(nil), manifest.Span.Key...),
			EndKey:    append(roachpb.Key(nil), manifest.Span.EndKey...),
			FileSize:  manifest.FileSize,
			RowSample: append(roachpb.Key(nil), manifest.RowSample...),
			KeyCount:  manifest.KeyCount,
		})
		result.TotalSize += manifest.FileSize
	}

	return result
}
