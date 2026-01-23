// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkutil

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
)

// DistMergePaths provides path construction for distributed merge operations.
// It centralizes the directory structure used by index backfill and IMPORT
// to avoid path proliferation across the codebase.
//
// Directory structure:
//   - Map phase: job/{jobID}/map/
//   - Merge iteration N: job/{jobID}/merge/iter-{N}/
type DistMergePaths struct {
	JobID jobspb.JobID
}

// NewDistMergePaths creates a DistMergePaths for the given job ID.
func NewDistMergePaths(jobID jobspb.JobID) DistMergePaths {
	return DistMergePaths{JobID: jobID}
}

// MapSubdir returns "map/" - the subdirectory for map phase SSTs.
func (p DistMergePaths) MapSubdir() string {
	return "map/"
}

// MergeSubdir returns the subdirectory for merge iteration output: "merge/iter-{N}/"
func (p DistMergePaths) MergeSubdir(iteration int) string {
	return fmt.Sprintf("merge/iter-%d/", iteration)
}

// InputSubdir returns the subdirectory containing INPUT files for iteration N.
// This is what needs cleanup after completing iteration N:
//   - iteration 1: returns "map/" (map phase was input)
//   - iteration N > 1: returns "merge/iter-{N-1}/"
func (p DistMergePaths) InputSubdir(iteration int) string {
	if iteration <= 1 {
		return p.MapSubdir()
	}
	return p.MergeSubdir(iteration - 1)
}

// JobPrefix returns "job/{jobID}/" - the job-scoped path prefix.
func (p DistMergePaths) JobPrefix() string {
	return fmt.Sprintf("job/%d/", p.JobID)
}

// MapPath returns the full map phase path: "job/{jobID}/map/"
func (p DistMergePaths) MapPath() string {
	return p.JobPrefix() + p.MapSubdir()
}

// MergePath returns the full merge iteration path: "job/{jobID}/merge/iter-{N}/"
func (p DistMergePaths) MergePath(iteration int) string {
	return p.JobPrefix() + p.MergeSubdir(iteration)
}
