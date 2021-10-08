// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
)

// TestingKnobs are knobs to inject behavior into the migration manager which
// are useful for testing.
type TestingKnobs struct {

	// ListBetweenOverride injects an override for `clusterversion.ListBetween()
	// in order to run migrations corresponding to versions which do not
	// actually exist.
	ListBetweenOverride func(from, to clusterversion.ClusterVersion) []clusterversion.ClusterVersion

	// RegistryOverride is used to inject migrations for specific cluster versions.
	RegistryOverride func(cv clusterversion.ClusterVersion) (Migration, bool)

	// BeforeWaitInRetryJobsWithExponentialBackoffMigration is called before
	// waiting for a mutation job to complete in retryJobsWithExponentialBackoff
	// migration.
	// TODO(sajjad): Remove this knob when the related migration code is removed.
	// This knob is used only in exponential backoff migration and related tests. See
	// pkg/migration/retry_jobs_with_exponential_backoff.go and
	// pkg/migration/retry_jobs_with_exponential_backoff_external_test.go
	BeforeWaitInRetryJobsWithExponentialBackoffMigration func(jobspb.JobID)

	// SkippedMutation is called if a mutation job is skipped as part of the
	// retryJobsWithExponentialBackoff migration.
	// TODO(sajjad): Remove this knob when the related migration code is removed.
	// This knob is used only in exponential backoff migration and related tests. See
	// pkg/migration/retry_jobs_with_exponential_backoff.go and
	// pkg/migration/retry_jobs_with_exponential_backoff_external_test.go
	SkippedMutation func()
}

// ModuleTestingKnobs makes TestingKnobs a base.ModuleTestingKnobs.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
