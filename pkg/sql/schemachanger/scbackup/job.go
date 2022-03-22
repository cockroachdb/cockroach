// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbackup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
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
	ctx context.Context, registry *jobs.Registry, txn *kv.Txn, allMut nstree.Catalog,
) error {
	byJobID := make(map[catpb.JobID][]catalog.MutableDescriptor)
	_ = allMut.ForEachDescriptorEntry(func(d catalog.Descriptor) error {
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
			descriptorStates = append(descriptorStates, ds)
		}
		// TODO(ajwerner): Deal with rollback and revertibility.
		currentState, err := scpb.MakeCurrentStateFromDescriptors(
			descriptorStates,
		)
		if err != nil {
			return err
		}
		records = append(records, scexec.MakeDeclarativeSchemaChangeJobRecord(
			newID,
			currentState.Statements,
			currentState.Authorization,
			screl.AllTargetDescIDs(currentState.TargetState).Ordered(),
		))
	}
	_, err := registry.CreateJobsWithTxn(ctx, txn, records)
	return err
}
