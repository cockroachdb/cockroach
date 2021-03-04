// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsprotectedts

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// MetaType is the value used in the ptpb.Record.MetaType field for records
// associated with jobs.
//
// This value must not be changed as it is used durably in the database.
const MetaType = "jobs"

// MakeStatusFunc returns a function which determines whether the job implied
// with this value of meta should be removed by the reconciler.
func MakeStatusFunc(jr *jobs.Registry) ptreconcile.StatusFunc {
	return func(ctx context.Context, txn *kv.Txn, meta []byte) (shouldRemove bool, _ error) {
		jobID, err := decodeJobID(meta)
		if err != nil {
			return false, err
		}
		j, err := jr.LoadJobWithTxn(ctx, jobID, txn)
		if jobs.HasJobNotFoundError(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		isTerminal := j.CheckTerminalStatus(ctx, txn)
		return isTerminal, nil
	}
}

// MakeRecord makes a protected timestamp record to protect a timestamp on
// behalf of this job.
func MakeRecord(
	id uuid.UUID, jobID jobspb.JobID, tsToProtect hlc.Timestamp, spans []roachpb.Span,
) *ptpb.Record {
	return &ptpb.Record{
		ID:        id,
		Timestamp: tsToProtect,
		Mode:      ptpb.PROTECT_AFTER,
		MetaType:  MetaType,
		Meta:      encodeJobID(jobID),
		Spans:     spans,
	}
}

func encodeJobID(jobID jobspb.JobID) []byte {
	return []byte(strconv.FormatInt(int64(jobID), 10))
}

func decodeJobID(meta []byte) (jobID jobspb.JobID, err error) {
	var id int64
	id, err = strconv.ParseInt(string(meta), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to interpret meta %q as bytes", meta)
	}
	return jobspb.JobID(id), err
}
