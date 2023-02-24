// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metricspoller

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const pausedJobsCountQuery = string(`
	SELECT job_type, count(*)
	FROM system.jobs
	WHERE status = '` + jobs.StatusPaused + `' 
  GROUP BY job_type`)

// updatePausedMetrics counts the number of paused jobs per job type.
func updatePausedMetrics(ctx context.Context, execCtx sql.JobExecContext) error {
	var metricUpdates map[jobspb.Type]int
	err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// In case of transaction retries, reset this map here.
		metricUpdates = make(map[jobspb.Type]int)

		// Run transaction at low priority to ensure that it does not
		// contend with foreground reads.
		if err := txn.KV().SetUserPriority(roachpb.MinUserPriority); err != nil {
			return err
		}
		rows, err := txn.QueryBufferedEx(
			ctx, "poll-jobs-metrics-job", txn.KV(), sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
			pausedJobsCountQuery,
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}

		for _, row := range rows {
			typeString := *row[0].(*tree.DString)
			count := *row[1].(*tree.DInt)
			typ, err := jobspb.TypeFromString(string(typeString))
			if err != nil {
				return err
			}
			metricUpdates[typ] = int(count)
		}

		return nil
	})
	if err != nil {
		return err
	}

	metrics := execCtx.ExecCfg().JobRegistry.MetricsStruct()
	for _, v := range jobspb.Type_value {
		if metrics.JobMetrics[v] != nil {
			metrics.JobMetrics[v].CurrentlyPaused.Update(int64(metricUpdates[jobspb.Type(v)]))
		}
	}
	return nil
}

// manageJobsProtectedTimestamps manages protected timestamp records owned by various jobs.
// This function mostly concerns itself with collecting statistics related to job PTS records.
// It also detects PTS records that are too old (as configured by the owner job) and requests
// job cancellation for those jobs.
func manageJobsProtectedTimestamps(ctx context.Context, execCtx sql.JobExecContext) error {
	type ptsStat struct {
		numRecords int64
		expired    int64
		oldest     hlc.Timestamp
	}
	var ptsStats map[jobspb.Type]*ptsStat

	execCfg := execCtx.ExecCfg()
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ptsStats = make(map[jobspb.Type]*ptsStat)
		ptsState, err := execCfg.ProtectedTimestampProvider.WithTxn(txn).GetState(ctx)
		if err != nil {
			return err
		}
		for _, rec := range ptsState.Records {
			if rec.MetaType != jobsprotectedts.GetMetaType(jobsprotectedts.Jobs) {
				continue
			}
			id, err := jobsprotectedts.DecodeID(rec.Meta)
			if err != nil {
				return err
			}
			j, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobspb.JobID(id), txn)
			if err != nil {
				continue
			}
			p := j.Payload()
			jobType, err := p.CheckType()
			if err != nil {
				return err
			}
			stats := ptsStats[jobType]
			if stats == nil {
				stats = &ptsStat{}
				ptsStats[jobType] = stats
			}
			stats.numRecords++
			if stats.oldest.IsEmpty() || rec.Timestamp.Less(stats.oldest) {
				stats.oldest = rec.Timestamp
			}

			// If MaximumPTSAge is set on the job payload, verify if PTS record
			// timestamp is fresh enough.  Note: we only look at paused jobs.
			// If the running job wants to enforce an invariant wrt to PTS age,
			// it can do so itself.  This check here is a safety mechanism to detect
			// paused jobs that own protected timestamp records.
			if j.Status() == jobs.StatusPaused &&
				p.MaximumPTSAge > 0 &&
				rec.Timestamp.GoTime().Add(p.MaximumPTSAge).Before(timeutil.Now()) {
				stats.expired++
				ptsExpired := errors.Newf(
					"protected timestamp records %s as of %s (age %s) exceeds job configured limit of %s",
					rec.ID, rec.Timestamp, timeutil.Since(rec.Timestamp.GoTime()), p.MaximumPTSAge)
				if err := j.WithTxn(txn).CancelRequestedWithReason(ctx, ptsExpired); err != nil {
					return err
				}
				log.Warningf(ctx, "job %d canceled due to %s", id, ptsExpired)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	jobMetrics := execCtx.ExecCfg().JobRegistry.MetricsStruct()
	for typ := 0; typ < jobspb.NumJobTypes; typ++ {
		if jobspb.Type(typ) == jobspb.TypeUnspecified { // do not track TypeUnspecified
			continue
		}
		m := jobMetrics.JobMetrics[typ]
		stats, found := ptsStats[jobspb.Type(typ)]
		if found {
			m.NumJobsWithPTS.Update(stats.numRecords)
			m.ExpiredPTS.Inc(stats.expired)
			if stats.oldest.WallTime > 0 {
				m.ProtectedAge.Update((execCfg.Clock.Now().WallTime - stats.oldest.WallTime) / 1e9)
			} else {
				m.ProtectedAge.Update(0)
			}
		} else {
			// If we haven't found PTS records for a job type, then reset stats.
			// (note: we don't reset counter based stats)
			m.NumJobsWithPTS.Update(0)
			m.ProtectedAge.Update(0)
		}
	}

	return nil
}
