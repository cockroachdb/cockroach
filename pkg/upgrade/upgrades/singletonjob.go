// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// singletonjob  provides infrastructure for creating singleton/static jobs.
// These jobs are automatically created when the cluster is initialized.

// staticJobID is a JobID for a singleton job.
// These IDs should *never* be reused.
// Add new static job definitions to the staticJobs map.
// If removing the job, add job ID to the retiredStaticJobIDs map, and
// implement migration code to remove old job.
type staticJobID struct {
	// static ID assigned to this job
	id jobspb.JobID
	// cluster version when this static job was added.
	// This is stored as a roachpb.Version proto instead of clusterversion.Key
	// to ensure that when clusterversion.Key is baked in, we still can determine
	// if a job needs to be created during bootstrap.
	cv roachpb.Version
}

// staticJob defines a job that must be created during cluster initialization.
// Each job must have associated cluster version which defines the version
// when this job is made available.
type staticJob struct {
	description string
	details     jobspb.Details
	progress    jobspb.ProgressDetails
	cancellable bool // By default, all static jobs are non-cancellable.
}

// Add static job definition here.
// It's okay to use toCV(clusterversion.Key) when specifying static job ID.
// However, once version is baked in, replace with the underlying roachpb.Version.
var staticJobs = map[staticJobID]staticJob{}

var retiredStaticJobIDs = map[jobspb.JobID]struct{}{}

// getStaticJobsUpgrades returns a list of upgrades to create static jobs.
// Each static job must have associated cluster version.  We return an entire
// list of upgrades because a) the list will likely tiny, and b) upgrade
// system skips previously executed upgrades and ignores upgrades that should not
// be performed due to version restrictions.
func getStaticJobsUpgrades() []upgrade.Upgrade {
	jobUpgrades := make([]upgrade.Upgrade, 0, len(staticJobs))
	for id := range staticJobs {
		jobUpgrades = append(jobUpgrades, newTenantUpgradeForStaticJob(id))
	}
	return jobUpgrades
}

const createdBy = "crdb_bootstrap"

func newTenantUpgradeForStaticJob(id staticJobID) upgrade.Upgrade {
	return upgrade.NewTenantUpgrade(
		fmt.Sprintf("add static job %d", id.id),
		clusterversion.ClusterVersion{Version: id.cv},
		NoPrecondition,
		func(ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps, _ *jobs.Job) error {
			return upsertStaticJobRecord(ctx, id, deps.JobRegistry)
		},
	)
}

// upsertStaticJobRecord upserts record for the specified static job.
func upsertStaticJobRecord(ctx context.Context, id staticJobID, registry *jobs.Registry) error {
	sj, ok := staticJobs[id]
	if !ok {
		return errors.AssertionFailedf("static job %d does not exist", id)
	}

	record := jobs.Record{
		JobID:         id.id,
		Description:   sj.description,
		Username:      username.RootUserName(),
		Details:       sj.details,
		Progress:      sj.progress,
		NonCancelable: !sj.cancellable,
		CreatedBy: &jobs.CreatedByInfo{
			Name: createdBy,
		},
	}
	const ifNotExist = true
	var noTxn *kv.Txn
	_, err := registry.CreateAdoptableJobWithTxnIfNotExist(ctx, ifNotExist, record, record.JobID, noTxn)
	return err
}

// deprecateStaticJob is intended to be used during migrations to request
// cancellation of the previously created static jobs for the purpose
// of deprecation.
func deprecateStaticJob(ctx context.Context, id jobspb.JobID, registry *jobs.Registry) error {
	if _, retired := retiredStaticJobIDs[id]; !retired {
		return errors.AssertionFailedf("cannot drop static job %d", id)
	}
	var noTxn *kv.Txn
	if err := registry.MustCancel(ctx, noTxn, jobspb.JobID(id)); err != nil {
		if jobs.HasJobNotFoundError(err) {
			return nil
		}
		return err
	}
	return nil
}

// TestingDeprecateStaticJob exploses deprecateStaticJob for testing.
func TestingDeprecateStaticJob(
	ctx context.Context, id jobspb.JobID, registry *jobs.Registry,
) error {
	return deprecateStaticJob(ctx, id, registry)
}

// TestingGetUpgradeForStaticJob returns upgrade object for static job.
func TestingGetUpgradeForStaticJob(
	jobID jobspb.JobID, cv roachpb.Version, details jobspb.Details, progress jobspb.ProgressDetails,
) upgrade.Upgrade {
	return newTenantUpgradeForStaticJob(staticJobID{id: jobID, cv: cv})
}

// TestingGetStaticJobIDs returns the set of configured static job IDs.
func TestingGetStaticJobIDs() (ids []jobspb.JobID) {
	for id := range staticJobs {
		ids = append(ids, id.id)
	}
	return ids
}

// TestingRetireStaticJob marks static job with specified ID as retired.
func TestingRetireStaticJob(id jobspb.JobID) func() {
	if _, retired := retiredStaticJobIDs[id]; retired {
		return func() {}
	}
	retiredStaticJobIDs[id] = struct{}{}
	return func() { delete(retiredStaticJobIDs, id) }
}
