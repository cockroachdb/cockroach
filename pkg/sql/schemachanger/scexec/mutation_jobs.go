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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type mutationJobs struct {
	jobs             []jobs.Record
	jobIDs           []jobspb.JobID
	jobRegistry      *jobs.Registry
	descriptorGCJobs []jobspb.SchemaChangeGCDetails_DroppedID
}

// CreateGCJobRecord creates the job record for a GC job, setting some
// properties which are common for all GC jobs.
func CreateGCJobRecord(
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

func (m *mutationJobs) AddNewGCJobForDescriptor(descriptor catalog.Descriptor) {
	m.descriptorGCJobs = append(m.descriptorGCJobs,
		jobspb.SchemaChangeGCDetails_DroppedID{
			ID:       descriptor.GetID(),
			DropTime: timeutil.Now().UnixNano(),
		})
}

func (m *mutationJobs) addNewGCJob(job jobspb.SchemaChangeGCDetails, description string) {
	record := CreateGCJobRecord(description, security.NodeUserName(), job)
	jobID := m.jobRegistry.MakeJobID()
	m.jobs = append(m.jobs, record)
	m.jobIDs = append(m.jobIDs, jobID)
}

func (m *mutationJobs) SubmitAllJobs(ctx context.Context, txn *kv.Txn) (bool, error) {
	if len(m.descriptorGCJobs) > 0 {
		job := jobspb.SchemaChangeGCDetails{
			Tables: m.descriptorGCJobs,
		}
		descriptorList := strings.Builder{}
		descriptorList.WriteString("Dropping descriptors ")
		for _, table := range m.descriptorGCJobs {
			descriptorList.WriteString(fmt.Sprintf("%d ", table.ID))
		}
		m.addNewGCJob(job, descriptorList.String())
		m.descriptorGCJobs = nil
	}
	for idx := range m.jobIDs {
		_, err := m.jobRegistry.CreateJobWithTxn(ctx, m.jobs[idx], m.jobIDs[idx], txn)
		if err != nil {
			return false, err
		}
	}
	return len(m.jobIDs) > 0, nil
}

var _ scmutationexec.MutationJobs = (*mutationJobs)(nil)
