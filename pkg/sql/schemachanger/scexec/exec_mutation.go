// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func executeDescriptorMutationOps(ctx context.Context, deps Dependencies, ops []scop.Op) error {
	mvs := newMutationVisitorState(deps.Catalog())
	v := scmutationexec.NewMutationVisitor(deps.Catalog(), mvs, deps.EventLogger())
	for _, op := range ops {
		if err := op.(scop.MutationOp).Visit(ctx, v); err != nil {
			return err
		}
	}
	b := deps.Catalog().NewCatalogChangeBatcher()
	err := mvs.checkedOutDescriptors.IterateByID(func(entry catalog.NameEntry) error {
		return b.CreateOrUpdateDescriptor(ctx, entry.(catalog.MutableDescriptor))
	})
	if err != nil {
		return err
	}
	for id, drainedNames := range mvs.drainedNames {
		for _, name := range drainedNames {
			if err := b.DeleteName(ctx, name, id); err != nil {
				return err
			}
		}
	}
	if len(mvs.descriptorGCJobs) > 0 {
		job := jobspb.SchemaChangeGCDetails{
			Tables: mvs.descriptorGCJobs,
		}
		descriptorList := strings.Builder{}
		descriptorList.WriteString("Dropping descriptors ")
		for _, table := range mvs.descriptorGCJobs {
			descriptorList.WriteString(fmt.Sprintf("%d ", table.ID))
		}
		record := createGCJobRecord(descriptorList.String(), security.NodeUserName(), job)
		if _, err := deps.TransactionalJobCreator().CreateJob(ctx, record); err != nil {
			return err
		}
	}
	if err := deps.EventLogger().ProcessAndSubmitEvents(ctx); err != nil {
		return err
	}
	return b.ValidateAndRun(ctx)
}

type mutationVisitorState struct {
	c                     Catalog
	checkedOutDescriptors nstree.Map
	drainedNames          map[descpb.ID][]descpb.NameInfo
	descriptorGCJobs      []jobspb.SchemaChangeGCDetails_DroppedID
}

func newMutationVisitorState(c Catalog) *mutationVisitorState {
	return &mutationVisitorState{
		c:            c,
		drainedNames: make(map[descpb.ID][]descpb.NameInfo),
	}
}

var _ scmutationexec.MutationVisitorStateUpdater = (*mutationVisitorState)(nil)

func (mvs *mutationVisitorState) CheckOutDescriptor(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	entry := mvs.checkedOutDescriptors.GetByID(id)
	if entry != nil {
		return entry.(catalog.MutableDescriptor), nil
	}
	mut, err := mvs.c.MustReadMutableDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut.MaybeIncrementVersion()
	mvs.checkedOutDescriptors.Upsert(mut)
	return mut, nil
}

func (mvs *mutationVisitorState) AddDrainedName(id descpb.ID, nameInfo descpb.NameInfo) {
	if _, ok := mvs.drainedNames[id]; !ok {
		mvs.drainedNames[id] = []descpb.NameInfo{nameInfo}
	} else {
		mvs.drainedNames[id] = append(mvs.drainedNames[id], nameInfo)
	}
}

func (mvs *mutationVisitorState) AddNewGCJobForDescriptor(descriptor catalog.Descriptor) {
	mvs.descriptorGCJobs = append(mvs.descriptorGCJobs,
		jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       descriptor.GetID(),
			DropTime: timeutil.Now().UnixNano(),
		})
}

// createGCJobRecord creates the job record for a GC job, setting some
// properties which are common for all GC jobs.
func createGCJobRecord(
	originalDescription string, username security.SQLUsername, details jobspb.SchemaChangeGCDetails,
) jobs.Record {
	descriptorIDs := make([]descpb.ID, 0)
	if len(details.Indexes) > 0 {
		if len(descriptorIDs) == 0 {
			descriptorIDs = []descpb.ID{details.ParentID}
		}
	} else {
		for _, table := range details.Tables {
			descriptorIDs = append(descriptorIDs, table.ID)
		}
	}
	return jobs.Record{
		Description:   fmt.Sprintf("GC for %s", originalDescription),
		Username:      username,
		DescriptorIDs: descriptorIDs,
		Details:       details,
		Progress:      jobspb.SchemaChangeGCProgress{},
		RunningStatus: "waiting for GC TTL",
		NonCancelable: true,
	}
}
