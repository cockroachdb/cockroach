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
)

// JobMetadataGetter is an interface used during job validation.
// It is similar in principle to validate.ValidationDereferencer.
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
		j, err := jmg.GetJobMetadata(m.JobID)
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
	j JobMetadata, descLookupFn func(id descpb.ID) catalog.Descriptor, errorAccFn func(error),
) {
	switch j.Status {
	case StatusRunning, StatusPaused, StatusPauseRequested:
		// Proceed.
	default:
		return
	}
	existing := catalog.MakeDescriptorIDSet()
	missing := catalog.MakeDescriptorIDSet()
	for _, id := range collectDescriptorReferences(j).Ordered() {
		if descLookupFn(id) != nil {
			existing.Add(id)
		} else if id != descpb.InvalidID {
			missing.Add(id)
		}
	}
	if missing.Len() == 0 {
		return
	}
	switch j.Payload.Type() {
	case jobspb.TypeSchemaChange:
		errorAccFn(errors.AssertionFailedf("%s schema change refers to missing descriptor(s) %+v",
			j.Status, missing.Ordered()))
	case jobspb.TypeSchemaChangeGC:
		isSafeToDelete := existing.Len() == 0 && len(j.Progress.GetSchemaChangeGC().Indexes) == 0
		errorAccFn(errors.AssertionFailedf("%s schema change GC refers to missing table descriptor(s) %+v; "+
			"existing descriptors that still need to be dropped %+v; job safe to delete: %v",
			j.Status, missing.Ordered(), existing.Ordered(), isSafeToDelete))
	case jobspb.TypeTypeSchemaChange:
		errorAccFn(errors.AssertionFailedf("%s type schema change refers to missing type descriptor %v",
			j.Status, missing.Ordered()))
	}
}

func collectDescriptorReferences(j JobMetadata) (ids catalog.DescriptorIDSet) {
	switch j.Payload.Type() {
	case jobspb.TypeSchemaChange:
		sc := j.Payload.GetSchemaChange()
		ids.Add(sc.DescID)
		ids.Add(sc.DroppedDatabaseID)
		for _, schemaID := range sc.DroppedSchemas {
			ids.Add(schemaID)
		}
		for _, typeID := range sc.DroppedTypes {
			ids.Add(typeID)
		}
		for _, table := range sc.DroppedTables {
			ids.Add(table.ID)
		}
	case jobspb.TypeSchemaChangeGC:
		for _, table := range j.Progress.GetSchemaChangeGC().Tables {
			if table.Status == jobspb.SchemaChangeGCProgress_DELETED {
				continue
			}
			ids.Add(table.ID)
		}
	case jobspb.TypeTypeSchemaChange:
		sc := j.Payload.GetTypeSchemaChange()
		ids.Add(sc.TypeID)
	}
	return ids
}
