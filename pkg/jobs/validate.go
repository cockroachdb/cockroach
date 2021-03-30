// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/sortkeys"
)

// JobMetadataGetter is an interface used during job validation.
// It is similar in principle to catalog.DescGetter.
type JobMetadataGetter interface {
	GetJobMetadata(jobspb.JobID) (*JobMetadata, error)
}

// ValidateJobReferencesInDescriptor checks a catalog.Descriptor for
// inconsistencies relative to system.jobs and passes any validation failures
// in the form of errors to an accumulator function.
func ValidateJobReferencesInDescriptor(
	desc catalog.Descriptor, jmg JobMetadataGetter, errorAccFn func(error),
) {
	// check for orphaned schema change jobs
	tbl, isTable := desc.(catalog.TableDescriptor)
	if !isTable {
		return
	}

	for _, m := range tbl.GetMutationJobs() {
		j, err := jmg.GetJobMetadata(jobspb.JobID(m.JobID))
		if err != nil {
			errorAccFn(errors.WithAssertionFailure(errors.Wrapf(err, "mutation job %d", m.JobID)))
			continue
		}
		if j == nil {
			errorAccFn(errors.AssertionFailedf("mutation job %d not found in system.jobs", m.JobID))
			continue
		}
		if j.Payload.Type() != jobspb.TypeSchemaChange {
			errorAccFn(errors.AssertionFailedf("mutation job %d is of type %q, expected schema change job", m.JobID, j.Payload.Type()))
		}
		if j.Status.Terminal() {
			errorAccFn(errors.AssertionFailedf("mutation job %d has terminal status (%s)", m.JobID, j.Status))
		}
	}
}

// ValidateDescriptorReferencesInJob checks a job for inconsistencies relative
// to system.descriptor and passes any validation failures in the form of errors
// to an accumulator function.
func ValidateDescriptorReferencesInJob(
	j JobMetadata, descMap map[descpb.ID]catalog.Descriptor, errorAccFn func(error),
) {
	if j.Status != StatusRunning {
		return
	}
	if j.Payload.Type() != jobspb.TypeSchemaChangeGC {
		return
	}
	existingTables := make([]int64, 0)
	missingTables := make([]int64, 0)
	for _, table := range j.Progress.GetSchemaChangeGC().Tables {
		if table.Status == jobspb.SchemaChangeGCProgress_DELETED {
			continue
		}
		_, tableExists := descMap[table.ID]
		if tableExists {
			existingTables = append(existingTables, int64(table.ID))
		} else {
			missingTables = append(missingTables, int64(table.ID))
		}
	}
	if len(missingTables) == 0 {
		return
	}

	sortkeys.Int64s(missingTables)
	isSafeToDelete := len(existingTables) == 0 && len(j.Progress.GetSchemaChangeGC().Indexes) == 0
	errorAccFn(errors.AssertionFailedf("schema change GC refers to missing table descriptor(s) %+v; "+
		"existing descriptors that still need to be dropped %+v; job safe to delete: %v",
		missingTables, existingTables, isSafeToDelete))
}
