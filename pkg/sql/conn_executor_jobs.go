// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// waitOneVersionForNewVersionDescriptorsWithoutJobs is used to wait until all
// descriptors with new versions to converge to one version in the cluster.
// `descIDsInJobs` are collected with `descIDsInSchemaChangeJobs`. We need to do
// this to make sure all descriptors mutated are at one version when the schema
// change finish in the user transaction. In schema change jobs, we do similar
// thing for affected descriptors. But, in some scenario, jobs are not created
// for mutated descriptors.
func (ex *connExecutor) waitOneVersionForNewVersionDescriptorsWithoutJobs(
	descIDsInJobs catalog.DescriptorIDSet, cachedRegions *regions.CachedDatabaseRegions,
) error {
	withNewVersion, err := ex.extraTxnState.descCollection.GetOriginalPreviousIDVersionsForUncommitted()
	if err != nil {
		return err
	}
	// If no schema change occurred, then nothing needs to be done here.
	if len(withNewVersion) == 0 {
		return nil
	}
	for _, idVersion := range withNewVersion {
		if descIDsInJobs.Contains(idVersion.ID) {
			continue
		}
		if _, err := WaitToUpdateLeases(ex.Ctx(), ex.planner.LeaseMgr(), cachedRegions, idVersion.ID); err != nil {
			// In most cases (normal schema changes), deleted descriptor should have
			// been handled by jobs. So, normally we won't hit into the situation of
			// wait for one version of a deleted descriptor. However, we need catch
			// ErrDescriptorNotFound here because we have a special case of descriptor
			// repairing where we delete descriptors directly and never record the ids
			// in jobs payload or details.
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				continue
			}
			return err
		}
	}
	return nil
}

func (ex *connExecutor) waitForInitialVersionForNewDescriptors(
	cachedRegions *regions.CachedDatabaseRegions,
) error {
	// Detect any tables that have just been created, we will confirm that all
	// nodes that have leased the schema for them out are aware of the new object.
	// This guarantees that any cached optimizer memos are discarded once the
	// user transaction completes.
	descriptorIDs := make(descpb.IDs, 0, len(ex.extraTxnState.descCollection.GetUncommittedTables()))
	for _, tbl := range ex.extraTxnState.descCollection.GetUncommittedTables() {
		if tbl.GetVersion() == 1 {
			descriptorIDs = append(descriptorIDs, tbl.GetID())
		}
	}
	return ex.planner.LeaseMgr().WaitForInitialVersion(ex.Ctx(), descriptorIDs, retry.Options{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Second,
		Multiplier:     1.5,
	}, cachedRegions)
}

// descIDsInSchemaChangeJobs returns all descriptor IDs with which schema change
// jobs in this transaction will perform. Within schema change jobs, we also
// wait until the whole cluster only has leases on the latest version of these
// descriptors, and we would like to also "wait for one version" for descriptors
// with new versions but not included in any schema change jobs.
func (ex *connExecutor) descIDsInSchemaChangeJobs() (catalog.DescriptorIDSet, error) {
	// Get descriptor IDs from legacy schema changer jobs.
	var descIDsInJobs catalog.DescriptorIDSet
	if err := ex.extraTxnState.jobs.forEachToCreate(func(jobRecord *jobs.Record) error {
		switch t := jobRecord.Details.(type) {
		case jobspb.SchemaChangeDetails:
			// In most cases, the field DescriptorIDs contains descriptor IDs the
			// schema change directly affects. Like it could be a table ID if an index
			// is created, or it could be a list of schema IDs when dropping a group
			// of schemas.
			// But it can be confusing sometimes in two types of schema change jobs:
			// (1) dropping a database:
			// In this scenario, the DescriptorIDs is the ids of tables in this
			// database that will be dropped together. And the DroppedDatabaseID field
			// is the actual ID of the database that will be dropped.
			// (2) any other changes on a database (but not drop):
			// For example, when renaming a schema, database's list of all schemas
			// need to be updated, and we create a job for this kind of database
			// changes. DescriptorIDs is empty in this case and the DescID field in
			// the job
			// detail is the actual database ID.
			for _, descID := range jobRecord.DescriptorIDs {
				descIDsInJobs.Add(descID)
			}
			for _, tbl := range t.DroppedTables {
				descIDsInJobs.Add(tbl.ID)
			}
			for _, id := range t.DroppedTypes {
				descIDsInJobs.Add(id)
			}
			for _, id := range t.DroppedSchemas {
				descIDsInJobs.Add(id)
			}
			descIDsInJobs.Add(t.DroppedDatabaseID)
			descIDsInJobs.Add(t.DescID)
		}
		return nil
	}); err != nil {
		return catalog.DescriptorIDSet{}, err
	}

	// If there is no declarative schema changer job, then we are done. Otherwise,
	// we need to check which descriptor has the jobID in its schema change state.
	if ex.extraTxnState.schemaChangerState.jobID == jobspb.InvalidJobID {
		return descIDsInJobs, nil
	}
	// Get descriptor IDs with declarative schema changer jobs.
	withNewVersion, err := ex.extraTxnState.descCollection.GetOriginalPreviousIDVersionsForUncommitted()
	if err != nil {
		return catalog.DescriptorIDSet{}, err
	}
	for _, idVersion := range withNewVersion {
		if descIDsInJobs.Contains(idVersion.ID) {
			continue
		}
		desc, err := ex.extraTxnState.descCollection.ByIDWithoutLeased(ex.state.mu.txn).Get().Desc(ex.Ctx(), idVersion.ID)
		if err != nil {
			return catalog.DescriptorIDSet{}, err
		}
		state := desc.GetDeclarativeSchemaChangerState()
		if state != nil && state.JobID != jobspb.InvalidJobID {
			descIDsInJobs.Add(idVersion.ID)
		}
	}
	return descIDsInJobs, nil
}
