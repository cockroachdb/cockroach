// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// jobDescriptionFromMutationID returns a string description of a mutation with
// a particular ID on a given tableDesc, as well as the number of mutations
// associated with it. This is only used to reconstruct a job based off a
// mutation, namely during RESTORE.
// N.B.: This is only to get an indication of what the schema change was trying
// to do and is not meant to be the exact same description/SQL that was used in
// the original job.
func jobDescriptionFromMutationID(
	tableDesc *sqlbase.TableDescriptor, id sqlbase.MutationID,
) (string, int, error) {
	var jobDescBuilder strings.Builder
	mutationCount := 0
	for _, m := range tableDesc.Mutations {
		if m.MutationID == id {
			mutationCount++
			// This is one of the mutations that we're looking for.
			// Note that for primary key swaps, we want the last mutation in this list.
			isPrimaryKeySwap := false
			switch m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_PrimaryKeySwap:
				isPrimaryKeySwap = true
			}

			if isPrimaryKeySwap {
				// Primary key swaps have multiple mutations with the same ID, but we
				// can derive the description from just the PrimaryKeySwap mutation
				// which appears after the other mutations.
				jobDescBuilder.Reset()
			} else if jobDescBuilder.Len() != 0 {
				jobDescBuilder.WriteString("; ")
			}

			if m.Rollback {
				jobDescBuilder.WriteString("rollback for ")
			}

			jobDescBuilder.WriteString(fmt.Sprintf("schema change on %s ", tableDesc.Name))

			if !isPrimaryKeySwap {
				switch m.Direction {
				case sqlbase.DescriptorMutation_ADD:
					jobDescBuilder.WriteString("adding ")
				case sqlbase.DescriptorMutation_DROP:
					jobDescBuilder.WriteString("dropping ")
				default:
					return "", 0, errors.Newf("unsupported mutation %+v, while restoring table %+v", m, tableDesc)
				}
			}

			switch t := m.Descriptor_.(type) {
			case *sqlbase.DescriptorMutation_Column:
				jobDescBuilder.WriteString("column ")
				jobDescBuilder.WriteString(t.Column.Name)
				if m.Direction == sqlbase.DescriptorMutation_ADD {
					jobDescBuilder.WriteString(" " + t.Column.Type.String())
				}
			case *sqlbase.DescriptorMutation_Index:
				jobDescBuilder.WriteString("index ")
				jobDescBuilder.WriteString(t.Index.Name + " for " + tableDesc.Name + " (")
				jobDescBuilder.WriteString(strings.Join(t.Index.ColumnNames, ", "))
				jobDescBuilder.WriteString(")")
			case *sqlbase.DescriptorMutation_Constraint:
				jobDescBuilder.WriteString("constraint ")
				jobDescBuilder.WriteString(t.Constraint.Name)
			case *sqlbase.DescriptorMutation_PrimaryKeySwap:
				jobDescBuilder.WriteString("changing primary key to (")
				newIndexID := t.PrimaryKeySwap.NewPrimaryIndexId
				// Find the ADD INDEX mutation with the same mutation ID that is adding
				// the new index.
				for _, otherMut := range tableDesc.Mutations {
					if indexMut, ok := otherMut.Descriptor_.(*sqlbase.DescriptorMutation_Index); ok &&
						indexMut.Index.ID == newIndexID &&
						otherMut.MutationID == m.MutationID &&
						m.Direction == sqlbase.DescriptorMutation_ADD {
						jobDescBuilder.WriteString(strings.Join(indexMut.Index.ColumnNames, ", "))
					}
				}
				jobDescBuilder.WriteString(")")
			default:
				return "", 0, errors.Newf("unsupported mutation %+v, while restoring table %+v", m, tableDesc)
			}
		}
	}

	jobDesc := jobDescBuilder.String()
	if mutationCount == 0 {
		return "", 0, errors.Newf("could not find mutation %d on table %s (%d) while restoring", id, tableDesc.Name, tableDesc.ID)
	}
	return jobDesc, mutationCount, nil
}

// createSchemaChangeJobsFromMutations creates and runs jobs for any mutations
// on the table descriptor. It also updates tableDesc's MutationJobs to
// reference the new jobs. This is only used to reconstruct a job based off a
// mutation, namely during RESTORE.
func createSchemaChangeJobsFromMutations(
	ctx context.Context,
	jr *jobs.Registry,
	codec keys.SQLCodec,
	txn *kv.Txn,
	username string,
	tableDesc *sqlbase.TableDescriptor,
) ([]*jobs.StartableJob, error) {
	mutationJobs := make([]sqlbase.TableDescriptor_MutationJob, 0, len(tableDesc.MutationJobs))
	newJobs := make([]*jobs.StartableJob, 0, len(tableDesc.MutationJobs))
	for _, mj := range tableDesc.MutationJobs {
		mutationID := mj.MutationID
		jobDesc, mutationCount, err := jobDescriptionFromMutationID(tableDesc, mj.MutationID)
		if err != nil {
			return nil, err
		}
		spanList := make([]jobspb.ResumeSpanList, mutationCount)
		for i := range spanList {
			spanList[i] = jobspb.ResumeSpanList{ResumeSpans: []roachpb.Span{tableDesc.PrimaryIndexSpan(codec)}}
		}
		jobRecord := jobs.Record{
			// We indicate that this schema change was triggered by a RESTORE since
			// the job description may not have all the information to fully describe
			// the schema change.
			Description:   "RESTORING: " + jobDesc,
			Username:      username,
			DescriptorIDs: sqlbase.IDs{tableDesc.GetID()},
			Details: jobspb.SchemaChangeDetails{
				TableID:        tableDesc.ID,
				MutationID:     mutationID,
				ResumeSpanList: spanList,
				FormatVersion:  jobspb.JobResumerFormatVersion,
			},
			Progress: jobspb.SchemaChangeProgress{},
		}
		newJob, err := jr.CreateStartableJobWithTxn(ctx, jobRecord, txn, nil)
		if err != nil {
			return nil, err
		}
		newMutationJob := sqlbase.TableDescriptor_MutationJob{
			MutationID: mutationID,
			JobID:      *newJob.ID(),
		}
		mutationJobs = append(mutationJobs, newMutationJob)
		newJobs = append(newJobs, newJob)

		log.Infof(ctx, "queued new schema change job %d for table %d, mutation %d",
			newJob.ID(), tableDesc.ID, mutationID)
	}
	tableDesc.MutationJobs = mutationJobs
	return newJobs, nil
}
