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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// CreateDeclarativeSchemaChangeJobs is called during the last phase of a
// restore. The provided catalog should contain all descriptors being restored.
// The code here will iterate those descriptors and synthesize the appropriate
// jobs.
//
// It should only be called for backups which do not restore the jobs table
// directly. schemaOnly reports whether the restore ingests no table data, in
// which case no restored schema change is settled.
func CreateDeclarativeSchemaChangeJobs(
	ctx context.Context,
	registry *jobs.Registry,
	txn isql.Txn,
	allMut nstree.Catalog,
	schemaOnly bool,
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
		// Settle the whole job if any of its descriptors depends on index data
		// RESTORE does not bring back; settling only some would leave an
		// inconsistent set of targets.
		settled := false
		if !schemaOnly && settleRestoredSchemaChanges.Get(&registry.ClusterSettings().SV) {
			for _, d := range descs {
				if dependsOnUnrestoredIndexData(d.GetDeclarativeSchemaChangerState(), restoredIndexIDs(d)) {
					settled = true
					break
				}
			}
		}
		var descriptorStates []*scpb.DescriptorState
		for _, d := range descs {
			ds := d.GetDeclarativeSchemaChangerState()
			ds.JobID = newID
			if settled {
				settle(ds)
			}
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

		runningStatus := "restored from backup"
		if settled {
			runningStatus = "restored from backup; settling to the state at backup time"
			// Plan the settled state the way the job will, so that a state the
			// planner rejects fails the RESTORE instead of leaving a job that can
			// neither finish nor be canceled.
			if _, err := scplan.MakePlan(ctx, currentState, scplan.Params{
				ActiveVersion:              registry.ClusterSettings().Version.ActiveVersion(ctx),
				InRollback:                 true,
				ExecutionPhase:             scop.PostCommitNonRevertiblePhase,
				SchemaChangerJobIDSupplier: func() jobspb.JobID { return newID },
				SkipPlannerSanityChecks:    true,
				MemAcc:                     mon.NewStandaloneUnlimitedAccount(),
			}); err != nil {
				return nil, errors.WithHintf(
					errors.Wrapf(err, "settling restored schema change job %d", newID),
					"setting %s to false restores the schema change without settling it",
					settleRestoredSchemaChanges.Name(),
				)
			}
		}
		record := scexec.MakeDeclarativeSchemaChangeJobRecord(
			newID,
			currentState.Statements,
			!currentState.Revertible, /* isNonCancelable */
			currentState.Authorization,
			descsToSchemaChange,
			runningStatus,
		)
		if settled {
			record.Description = SettledJobDescriptionPrefix + record.Description
			log.Warningf(ctx, "settling restored schema change job %d on descriptors %v to the "+
				"state at backup time: it depends on index data the restore does not bring back",
				newID, &descsToSchemaChange)
		}
		records = append(records, record)
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
