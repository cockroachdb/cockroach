// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkutil

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
)

// BulkJobCleaner provides generic cleanup operations for bulk jobs that write
// temporary files to external storage (e.g., IMPORT, index backfill). It
// efficiently deletes files and sweeps job directories using cached storage
// handles.
//
// Cleanup operations are best-effort; callers should log returned errors
// rather than failing the job.
type BulkJobCleaner struct {
	mux *ExternalStorageMux
}

// NewBulkJobCleaner builds a cleaner that reuses external storage handles
// across delete/list operations.
func NewBulkJobCleaner(
	factory cloud.ExternalStorageFromURIFactory, user username.SQLUsername,
) *BulkJobCleaner {
	return &BulkJobCleaner{
		mux: NewExternalStorageMux(factory, user),
	}
}

// Close releases any cached external storage handles.
func (c *BulkJobCleaner) Close() error {
	if c == nil {
		return nil
	}
	return c.mux.Close()
}

// CleanupURIs deletes the provided URIs. The operation is best-effort; errors
// are aggregated and returned.
func (c *BulkJobCleaner) CleanupURIs(ctx context.Context, uris []string) error {
	if c == nil {
		return nil
	}
	var errOut error
	for _, uri := range uris {
		if uri == "" {
			continue
		}
		if err := c.mux.DeleteFile(ctx, uri); err != nil {
			errOut = errors.CombineErrors(errOut, err)
		}
	}
	return errOut
}

// CleanupJobDirectories enumerates all files under the job-scoped directories
// and removes them. This is intended as a catch-all sweep after targeted
// cleanup has already run.
//
// The storagePrefixes parameter specifies the storage locations (without the
// job directory path) where temporary files may exist. The function constructs
// the full cleanup path as "<prefix>/job/<jobID>/" and removes all files under
// that path.
//
// Examples:
//
//   - Input: storagePrefixes=["nodelocal://1/", "nodelocal://2/"], jobID=123
//     Cleans: "nodelocal://1/job/123/*" and "nodelocal://2/job/123/*"
//
//   - Input: storagePrefixes=["nodelocal://1/export/"], jobID=456
//     Cleans: "nodelocal://1/export/job/456/*"
//
// These prefixes should be persisted in job state before any files are written
// to ensure complete cleanup even if the job fails partway through.
func (c *BulkJobCleaner) CleanupJobDirectories(
	ctx context.Context, jobID jobspb.JobID, storagePrefixes []string,
) error {
	if c == nil {
		return nil
	}

	// Construct full job directory paths from storage prefixes.
	jobDirs := make([]string, 0, len(storagePrefixes))
	for _, prefix := range storagePrefixes {
		if prefix == "" {
			continue
		}
		// Ensure prefix ends with / before appending job path.
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		jobDir := fmt.Sprintf("%sjob/%d/", prefix, jobID)
		jobDirs = append(jobDirs, jobDir)
	}

	// Remove duplicates.
	seen := make(map[string]struct{})
	var uniqueDirs []string
	for _, dir := range jobDirs {
		if _, exists := seen[dir]; !exists {
			seen[dir] = struct{}{}
			uniqueDirs = append(uniqueDirs, dir)
		}
	}

	var errOut error
	for _, jobDir := range uniqueDirs {
		listErr := c.mux.ListFiles(ctx, jobDir, func(name string) error {
			trimmed := strings.TrimPrefix(name, "/")
			target := jobDir
			if !strings.HasSuffix(target, "/") {
				target += "/"
			}
			target += trimmed
			if err := c.mux.DeleteFile(ctx, target); err != nil {
				errOut = errors.CombineErrors(errOut, err)
			}
			return nil
		})
		if errors.Is(listErr, cloud.ErrListingUnsupported) {
			continue
		}
		if listErr != nil {
			errOut = errors.CombineErrors(errOut, listErr)
		}
	}
	return errOut
}
