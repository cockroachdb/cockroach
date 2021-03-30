package jobs

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/sortkeys"
)

func ValidateJobReferencesInDescriptor(
	desc catalog.Descriptor, jobMap map[int64]JobMetadata, errorAccFn func(error),
) {
	// check for orphaned schema change jobs
	tbl, isTable := desc.(catalog.TableDescriptor)
	if !isTable {
		return
	}

	for _, m := range tbl.GetMutationJobs() {
		j, found := jobMap[m.JobID]
		if !found {
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
