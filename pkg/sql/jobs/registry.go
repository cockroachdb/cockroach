// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package jobs

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// Registry creates Jobs, and will soon manage their leases and cancelation.
type Registry struct {
	db *client.DB
	ex sqlutil.InternalExecutor
}

// MakeRegistry creates a new Registry.
func MakeRegistry(db *client.DB, ex sqlutil.InternalExecutor) *Registry {
	return &Registry{db: db, ex: ex}
}

// NewJob creates a new Job.
func (r *Registry) NewJob(record Record) *Job {
	return &Job{
		Record:   record,
		registry: r,
	}
}

// LoadJob loads an existing job with the given jobID from the system.jobs
// table.
func (r *Registry) LoadJob(ctx context.Context, jobID int64) (*Job, error) {
	j := &Job{
		id:       &jobID,
		registry: r,
	}
	if err := j.load(ctx); err != nil {
		return nil, err
	}
	return j, nil
}

// LoadJobWithTxn does the same as above, but using the transaction passed in
// the txn argument.
func (r *Registry) LoadJobWithTxn(ctx context.Context, jobID int64, txn *client.Txn) (*Job, error) {
	j := &Job{
		id:       &jobID,
		registry: r,
	}
	j = j.WithTxn(txn)
	if err := j.load(ctx); err != nil {
		return nil, err
	}
	return j, nil
}
