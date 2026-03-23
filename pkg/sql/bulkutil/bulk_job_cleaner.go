// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkutil

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	return c.mux.Close()
}

// CleanupURIs deletes the provided URIs. The operation is best-effort; errors
// are aggregated and returned.
func (c *BulkJobCleaner) CleanupURIs(ctx context.Context, uris []string) error {
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

// ensureTrailingSlash returns the string with a trailing slash if it doesn't
// already have one.
func ensureTrailingSlash(s string) string {
	if !strings.HasSuffix(s, "/") {
		return s + "/"
	}
	return s
}

// dedupeDirectories removes duplicate directory paths while preserving order.
func dedupeDirectories(dirs []string) []string {
	seen := make(map[string]struct{})
	var uniqueDirs []string
	for _, dir := range dirs {
		if _, exists := seen[dir]; !exists {
			seen[dir] = struct{}{}
			uniqueDirs = append(uniqueDirs, dir)
		}
	}
	return uniqueDirs
}

// deleteFilesInDirectories lists and deletes all files in the given
// directories. Errors are aggregated and returned. Directories that don't
// support listing are skipped.
func (c *BulkJobCleaner) deleteFilesInDirectories(ctx context.Context, dirs []string) error {
	var errOut error
	for _, dir := range dirs {
		listErr := c.mux.ListFiles(ctx, dir, func(name string) error {
			relativePath := strings.TrimPrefix(name, "/")
			target := ensureTrailingSlash(dir) + relativePath
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
	// Construct full job directory paths from storage prefixes.
	jobDirs := make([]string, 0, len(storagePrefixes))
	for _, prefix := range storagePrefixes {
		if prefix == "" {
			continue
		}
		jobDir := fmt.Sprintf("%sjob/%d/", ensureTrailingSlash(prefix), jobID)
		jobDirs = append(jobDirs, jobDir)
	}

	uniqueDirs := dedupeDirectories(jobDirs)
	return c.deleteFilesInDirectories(ctx, uniqueDirs)
}

// CleanupJobSubdirectory enumerates all files under a specific subdirectory
// within the job-scoped directories and removes them. This is intended for
// cleaning up intermediate files between merge iterations.
//
// The storagePrefixes parameter specifies the storage locations (without the
// job directory path) where temporary files may exist. The function constructs
// the full cleanup path as "<prefix>/job/<jobID>/<subdirectory>" and removes
// all files under that path.
func (c *BulkJobCleaner) CleanupJobSubdirectory(
	ctx context.Context, jobID jobspb.JobID, storagePrefixes []string, subdirectory string,
) error {
	// Construct full subdirectory paths from storage prefixes.
	subDirs := make([]string, 0, len(storagePrefixes))
	for _, prefix := range storagePrefixes {
		if prefix == "" {
			continue
		}
		subDir := fmt.Sprintf("%sjob/%d/%s", ensureTrailingSlash(prefix), jobID, subdirectory)
		subDirs = append(subDirs, subDir)
	}

	uniqueDirs := dedupeDirectories(subDirs)
	return c.deleteFilesInDirectories(ctx, uniqueDirs)
}

// CleanupOrphanedFiles lists job subdirectories under nodelocal://self/job/,
// checks each job's status, and deletes files for jobs that are terminal or no
// longer exist.
func CleanupOrphanedFiles(
	ctx context.Context, factory cloud.ExternalStorageFromURIFactory, db isql.DB,
) error {
	// List subdirectories under nodelocal://self/job/.
	const jobDirURI = "nodelocal://self/job/"
	mux := NewExternalStorageMux(factory, username.RootUserName())
	defer func() {
		if err := mux.Close(); err != nil {
			log.Dev.Warningf(ctx, "error closing storage mux during bulk file cleanup: %v", err)
		}
	}()

	// Collect job ID directories.
	var jobIDs []jobspb.JobID
	listErr := mux.ListDirectories(ctx, jobDirURI, func(name string) error {
		// Directory entries have trailing slash, e.g. "123/".
		name = strings.TrimSuffix(name, "/")
		id, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			log.Dev.Warningf(ctx, "skipping non-numeric directory in job path: %q", name)
			return nil //nolint:returnerrcheck
		}
		jobIDs = append(jobIDs, jobspb.JobID(id))
		return nil
	})
	if listErr != nil {
		log.Dev.Warningf(ctx, "error listing job directories: %v", listErr)
		return listErr
	}

	if len(jobIDs) == 0 {
		return nil
	}

	var cleaned, skipped int
	cleaner := NewBulkJobCleaner(factory, username.RootUserName())
	defer func() {
		if err := cleaner.Close(); err != nil {
			log.Dev.Warningf(ctx, "error closing bulk job cleaner: %v", err)
		}
	}()

	for _, jobID := range jobIDs {
		shouldClean, err := shouldCleanJob(ctx, db, jobID)
		if err != nil {
			log.Dev.Warningf(ctx, "error checking status of job %d, skipping: %v", jobID, err)
			skipped++
			continue
		}
		if !shouldClean {
			skipped++
			continue
		}

		if err := cleaner.CleanupJobDirectories(
			ctx, jobID, []string{"nodelocal://self/"},
		); err != nil {
			log.Dev.Warningf(ctx, "error cleaning files for job %d: %v", jobID, err)
			skipped++
			continue
		}
		cleaned++
	}

	log.Dev.Infof(ctx, "bulk file cleanup complete: cleaned %d job directories, skipped %d", cleaned, skipped)
	return nil
}

// shouldCleanJob checks whether the given job's files should be cleaned up.
// Returns true if the job is in a terminal state or does not exist.
func shouldCleanJob(ctx context.Context, db isql.DB, jobID jobspb.JobID) (bool, error) {
	row, err := db.Executor().QueryRowEx(
		ctx,
		"bulk-file-cleaner-check-job",
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		"SELECT status FROM system.jobs WHERE id = $1",
		jobID,
	)
	if err != nil {
		return false, err
	}

	// Job does not exist; safe to clean up.
	if row == nil {
		return true, nil
	}

	status := jobs.State(tree.MustBeDString(row[0]))
	return status.Terminal(), nil
}
