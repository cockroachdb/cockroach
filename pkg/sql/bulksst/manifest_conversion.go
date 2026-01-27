// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// SSTFileToManifest converts a single bulksst.SSTFileInfo to a
// jobspb.IndexBackfillSSTManifest for persistence in job progress.
//
// The writeTS parameter is optional (can be nil). When non-nil, it's stored in
// the manifest's WriteTimestamp field. This is used by index backfill but not
// by import operations.
func SSTFileToManifest(
	f *SSTFileInfo, writeTS *hlc.Timestamp,
) jobspb.IndexBackfillSSTManifest {
	span := &roachpb.Span{
		Key:    append(roachpb.Key(nil), f.StartKey...),
		EndKey: append(roachpb.Key(nil), f.EndKey...),
	}
	manifest := jobspb.IndexBackfillSSTManifest{
		URI:       f.URI,
		Span:      span,
		FileSize:  f.FileSize,
		RowSample: append(roachpb.Key(nil), f.RowSample...),
		KeyCount:  f.KeyCount,
	}
	if writeTS != nil {
		manifest.WriteTimestamp = writeTS
	}
	return manifest
}

// SSTFilesToManifests converts a slice of bulksst.SSTFileInfo to
// jobspb.IndexBackfillSSTManifest entries for persistence.
//
// This is used by both import and index backfill operations to convert their
// in-memory SST file lists to the format stored in job progress. The writeTS
// parameter is optional and only used by index backfill.
func SSTFilesToManifests(
	files *SSTFiles, writeTS *hlc.Timestamp,
) []jobspb.IndexBackfillSSTManifest {
	if files == nil || len(files.SST) == 0 {
		return nil
	}

	manifests := make([]jobspb.IndexBackfillSSTManifest, 0, len(files.SST))
	for _, f := range files.SST {
		manifests = append(manifests, SSTFileToManifest(f, writeTS))
	}
	return manifests
}

// ManifestsToSSTFiles converts jobspb.IndexBackfillSSTManifest entries back to
// bulksst.SSTFiles. This is the inverse of SSTFilesToManifests.
//
// This is used when restoring SST metadata from job progress during retry or
// resume operations.
func ManifestsToSSTFiles(manifests []jobspb.IndexBackfillSSTManifest) SSTFiles {
	if len(manifests) == 0 {
		return SSTFiles{}
	}

	result := SSTFiles{
		SST: make([]*SSTFileInfo, 0, len(manifests)),
	}

	for _, manifest := range manifests {
		fileInfo := &SSTFileInfo{
			URI:       manifest.URI,
			StartKey:  append(roachpb.Key(nil), manifest.Span.Key...),
			EndKey:    append(roachpb.Key(nil), manifest.Span.EndKey...),
			FileSize:  manifest.FileSize,
			RowSample: append(roachpb.Key(nil), manifest.RowSample...),
			KeyCount:  manifest.KeyCount,
		}
		result.SST = append(result.SST, fileInfo)
		result.TotalSize += manifest.FileSize
	}

	return result
}
