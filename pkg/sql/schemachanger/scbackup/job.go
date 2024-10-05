// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbackup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

// CreateDeclarativeSchemaChangeJobs is called during the last phase of a
// restore. The provided catalog should contain all descriptors being restored.
// The code here will iterate those descriptors and synthesize the appropriate
// jobs.
//
// It should only be called for backups which do not restore the jobs table
// directly.
func CreateDeclarativeSchemaChangeJobs(
	ctx context.Context, registry *jobs.Registry, txn isql.Txn, allMut nstree.Catalog,
) ([]jobspb.JobID, error) {
	byJobID := make(map[catpb.JobID][]catalog.MutableDescriptor)
	_ = allMut.ForEachDescriptor(func(d catalog.Descriptor) error {
		if s := d.GetDeclarativeSchemaChangerState(); s != nil {
			byJobID[s.JobID] = append(byJobID[s.JobID], d.(catalog.MutableDescriptor))
		}
		return nil
	})
	var records []*jobs.Record
	for _, descs := range byJobID {
		// TODO(ajwerner): Consider the need to trim elements or update
		// descriptors in the face of restoring only some constituent
		// descriptors of a larger change. One example where this needs
		// to happen urgently is sequences. Others shouldn't be possible
		// at this point.
		newID := registry.MakeJobID()
		var descriptorStates []*scpb.DescriptorState
		for _, d := range descs {
			ds := d.GetDeclarativeSchemaChangerState()
			ds.JobID = newID
			d.SetDeclarativeSchemaChangerState(ds)
			descriptorStates = append(descriptorStates, ds)
		}
		currentState, err := scpb.MakeCurrentStateFromDescriptors(
			descriptorStates,
		)
		if err != nil {
			return nil, err
		}

		// If all targets have reached their target status, simply clear the
		// declarative schema changer state on all descriptors and skip creating a
		// declarative schema changer job.
		if haveAllTargetsReachedTheirTargetStatus(currentState) {
			for _, desc := range descs {
				desc.SetDeclarativeSchemaChangerState(nil)
			}
			continue
		}
		// If a descriptor has zero targets, and it does not need to be schema
		// changed, then clear its declarative schema changer state. This can
		// happen, for example, if a dropped schema clears the schemaID in its
		// parent database but was excluded from the BACKUP (bc it's in DROP state),
		// and the parent database will have a non-nil but empty declarative schema
		// changer state, which we can safely clear out.
		descsToSchemaChange := screl.AllTargetStateDescIDs(currentState.TargetState)
		for _, desc := range descs {
			if len(desc.GetDeclarativeSchemaChangerState().Targets) == 0 &&
				!descsToSchemaChange.Contains(desc.GetID()) {
				desc.SetDeclarativeSchemaChangerState(nil)
			}
		}

		const runningStatus = "restored from backup"
		records = append(records, scexec.MakeDeclarativeSchemaChangeJobRecord(
			newID,
			currentState.Statements,
			!currentState.Revertible, /* isNonCancelable */
			currentState.Authorization,
			screl.AllTargetStateDescIDs(currentState.TargetState),
			runningStatus,
		))
	}
	jobIDs, err := registry.CreateJobsWithTxn(ctx, txn, records)
	return jobIDs, err
}

// haveAllTargetsReachedTheirTargetStatus determines whether all targets have
// reached their target status.
func haveAllTargetsReachedTheirTargetStatus(initial scpb.CurrentState) bool {
	for i, status := range initial.Current {
		if status != initial.Targets[i].TargetStatus {
			return false
		}
	}
	return true
}
