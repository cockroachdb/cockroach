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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptreconcile"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// MetaType represents the types of meta values we support for records
// associated with jobs or schedules.
type MetaType int

const (
	// Jobs is the meta type for records associated with jobs.
	Jobs MetaType = iota
	// Schedules is the meta type for records associated with schedules.
	Schedules
)

// The value of metaTypes is used in the ptpb.Record.MetaType field for records
// associated with jobs/schedules.
//
// These values must not be changed as it is used durably in the database.
var metaTypes = map[MetaType]string{Jobs: "jobs", Schedules: "schedules"}

// GetMetaType return the value for the provided metaType that is used in the
// ptpb.Record.MetaType field for records associated with jobs/schedules.
func GetMetaType(metaType MetaType) string {
	return metaTypes[metaType]
}

// MakeStatusFunc returns a function which determines whether the job or
// schedule implied with this value of meta should be removed by the reconciler.
func MakeStatusFunc(jr *jobs.Registry, metaType MetaType) ptreconcile.StatusFunc {
	switch metaType {
	case Jobs:
		return func(ctx context.Context, txn isql.Txn, meta []byte) (shouldRemove bool, _ error) {
			jobID, err := decodeID(meta)
			if err != nil {
				return false, err
			}
			j, err := jr.LoadJobWithTxn(ctx, jobspb.JobID(jobID), txn)
			if jobs.HasJobNotFoundError(err) {
				return true, nil
			}
			if err != nil {
				return false, err
			}
			isTerminal := j.WithTxn(txn).CheckTerminalStatus(ctx)
			return isTerminal, nil
		}
	case Schedules:
		return func(ctx context.Context, txn isql.Txn, meta []byte) (shouldRemove bool, _ error) {
			scheduleID, err := decodeID(meta)
			if err != nil {
				return false, err
			}
			_, err = jobs.ScheduledJobTxn(txn).
				Load(ctx, scheduledjobs.ProdJobSchedulerEnv, scheduleID)
			if jobs.HasScheduledJobNotFoundError(err) {
				return true, nil
			}
			return false, err
		}
	}
	return nil
}

// MakeRecord makes a protected timestamp record to protect a timestamp on
// behalf of this job.
//
// TODO(adityamaru): In 22.2 stop passing `deprecatedSpans` since PTS records
// will stop protecting key spans.
func MakeRecord(
	recordID uuid.UUID,
	metaID int64,
	tsToProtect hlc.Timestamp,
	deprecatedSpans []roachpb.Span,
	metaType MetaType,
	target *ptpb.Target,
) *ptpb.Record {
	return &ptpb.Record{
		ID:              recordID.GetBytesMut(),
		Timestamp:       tsToProtect,
		Mode:            ptpb.PROTECT_AFTER,
		MetaType:        metaTypes[metaType],
		Meta:            encodeID(metaID),
		DeprecatedSpans: deprecatedSpans,
		Target:          target,
	}
}

func encodeID(id int64) []byte {
	return []byte(strconv.FormatInt(id, 10))
}

func decodeID(meta []byte) (id int64, err error) {
	id, err = strconv.ParseInt(string(meta), 10, 64)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to interpret meta %q as bytes", meta)
	}
	return id, err
}
