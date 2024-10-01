// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

type jobsCollection []jobspb.JobID

func (jc *jobsCollection) add(ids ...jobspb.JobID) {
	*jc = append(*jc, ids...)
}

func (jc *jobsCollection) reset() {
	*jc = nil
}

// txnJobsCollection is used to collect information of all jobs created in a
// transaction. It's also used as a cache of job records created outside
// declarative schema changer.
type txnJobsCollection struct {
	// created represents a list of job IDs that has been created and queued to
	// system.jobs.
	created jobsCollection
	// uniqueToCreate contains job records unique to a descriptor ID. Typically,
	// this kind of jobs are created when mutating relations, we only allow one
	// job for a relation in one transaction. These jobs will be created and
	// queued at commit time.
	uniqueToCreate map[descpb.ID]*jobs.Record
	// nonUniqueToCreate contains job records that are not unique to a descriptor
	// IDs. These jobs will be created and queued at commit time.
	nonUniqueToCreate []*jobs.Record
}

func newTxnJobsCollection() *txnJobsCollection {
	ret := &txnJobsCollection{
		uniqueToCreate: make(map[descpb.ID]*jobs.Record),
	}
	return ret
}

func (j *txnJobsCollection) addCreatedJobID(jobID ...jobspb.JobID) {
	j.created.add(jobID...)
}

func (j *txnJobsCollection) addNonUniqueJobToCreate(jobRecord *jobs.Record) {
	j.nonUniqueToCreate = append(j.nonUniqueToCreate, jobRecord)
}

func (j *txnJobsCollection) reset() {
	j.created.reset()
	for id := range j.uniqueToCreate {
		delete(j.uniqueToCreate, id)
	}
	j.nonUniqueToCreate = nil
}

func (j *txnJobsCollection) numToCreate() int {
	return len(j.uniqueToCreate) + len(j.nonUniqueToCreate)
}

func (j *txnJobsCollection) hasAnyToCreate() bool {
	return j.numToCreate() > 0
}

func (j *txnJobsCollection) forEachToCreate(fn func(jobRecord *jobs.Record) error) error {
	for _, r := range j.uniqueToCreate {
		if err := fn(r); err != nil {
			return err
		}
	}
	for _, r := range j.nonUniqueToCreate {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}
