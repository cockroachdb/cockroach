// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metricspoller

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
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
	WHERE status = '` + jobs.StatePaused + `'
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
			ctx, "poll-jobs-metrics-job", txn.KV(), sessiondata.NodeUserSessionDataOverride,
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

// lowTSForTypeQuery finds the lowest non-null ts for all jobs of a given type.
// min() ignores nulls; jobs of the type that do not have a ts are not included
// in the result, e.g. a new changefeed that is still in initial scan would not
// change the result of the query, but if after its initial scan it was several
// minutes behind and was the most lagged changefeed, that would be reflected.
const lowTSForTypeQuery = `SELECT min(high_water_timestamp) FROM crdb_internal.jobs WHERE job_type = $1 AND status IN ` + jobs.NonTerminalStateTupleString

// updateTSMetrics updates the metrics for jobs that have registered for ts
// tracking.
func updateTSMetrics(ctx context.Context, execCtx sql.JobExecContext) error {
	for _, typ := range jobspb.Type_value {
		m := execCtx.ExecCfg().JobRegistry.MetricsStruct().ResolvedMetrics[typ]
		// If this job type does not register a resolved TS metric, skip it.
		if m == nil {
			continue
		}

		var ts hlc.Timestamp
		if err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			if err := txn.KV().SetUserPriority(roachpb.MinUserPriority); err != nil {
				return err
			}
			row, err := txn.QueryRowEx(
				ctx, "poll-jobs-metrics-ts", txn.KV(), sessiondata.NodeUserSessionDataOverride,
				lowTSForTypeQuery, jobspb.Type(typ).String(),
			)
			// Feeding zero non-null rows to min() returns null; return and record
			// a zero ts (i.e. no data.) in this case or an error case.
			if err != nil || row == nil || row[0] == tree.DNull {
				return err
			}
			d := *row[0].(*tree.DDecimal)
			ts, err = hlc.DecimalToHLC(&d.Decimal)
			return err
		}); err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		m.Update(ts.GoTime().Unix())
	}
	return nil
}

type ptsStat struct {
	numRecords int64
	expired    int64
	oldest     hlc.Timestamp
}

type schedulePTSStat struct {
	ptsStat
	m *jobs.ExecutorPTSMetrics
}

const cancelJobTimeout = 60 * time.Second

// manageProtectedTimestamps manages protected timestamp records owned by
// various jobs or schedules.. This function mostly concerns itself with
// collecting statistics related to job PTS records. It also detects PTS records
// that are too old (as configured by the owner job) and requests job
// cancellation for those jobs.
func manageProtectedTimestamps(ctx context.Context, execCtx sql.JobExecContext) error {
	var ptsStats map[jobspb.Type]*ptsStat
	var schedulePtsStats map[string]*schedulePTSStat
	var ptsState ptpb.State
	execCfg := execCtx.ExecCfg()

	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ptsStats = make(map[jobspb.Type]*ptsStat)
		schedulePtsStats = make(map[string]*schedulePTSStat)
		var err error
		ptsState, err = execCfg.ProtectedTimestampProvider.WithTxn(txn).GetState(ctx)
		return err
	}); err != nil {
		return err
	}

	// Iterate over each job with a fresh transaction, to avoid reading and
	// updating too many jobs in a single transaction.
	for _, scannedRec := range ptsState.Records {
		id, err := jobsprotectedts.DecodeID(scannedRec.Meta)
		if err != nil {
			return err
		}
		if err := timeutil.RunWithTimeout(ctx, "cancel-job-old-pts", cancelJobTimeout, func(ctx context.Context) error {
			return execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				// Grab the pts within the transaction to ensure we have an up to date view of it.
				rec, err := execCfg.ProtectedTimestampProvider.WithTxn(txn).GetRecord(ctx, scannedRec.ID.GetUUID())
				if err != nil {
					return err
				}
				switch rec.MetaType {
				case jobsprotectedts.GetMetaType(jobsprotectedts.Jobs):
					if err := processJobPTSRecord(ctx, execCfg, id, rec, ptsStats, txn); err != nil {
						return err
					}
				case jobsprotectedts.GetMetaType(jobsprotectedts.Schedules):
					if err := processSchedulePTSRecord(ctx, jobspb.ScheduleID(id), rec, schedulePtsStats, txn); err != nil {
						return err
					}
				}
				return nil
			})
		}); err != nil {
			// If we fail to process one record, we should still try to process
			// subsequent records, therefore, just log the error instead of returning
			// early.
			log.Infof(ctx, "could not process pts record id %d: %s", scannedRec.ID, err.Error())
		}
	}

	updateJobPTSMetrics(execCfg.JobRegistry.MetricsStruct(), execCfg.Clock, ptsStats)
	updateSchedulesPTSMetrics(execCfg.Clock, schedulePtsStats)

	return nil
}

func processJobPTSRecord(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	jobID int64,
	rec *ptpb.Record,
	ptsStats map[jobspb.Type]*ptsStat,
	txn isql.Txn,
) error {
	var stats *ptsStat
	defer func() {
		if stats != nil {
			stats.numRecords++
			if stats.oldest.IsEmpty() || rec.Timestamp.Less(stats.oldest) {
				stats.oldest = rec.Timestamp
			}
		}
	}()

	err := execCfg.JobRegistry.UpdateJobWithTxn(ctx, jobspb.JobID(jobID), txn,
		func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			p := md.Payload
			jobType, err := p.CheckType()
			if err != nil {
				return err
			}
			stats = ptsStats[jobType]
			if stats == nil {
				stats = &ptsStat{}
				ptsStats[jobType] = stats
			}

			// If MaximumPTSAge is set on the job payload, verify if PTS record
			// timestamp is fresh enough.
			if p.MaximumPTSAge > 0 &&
				rec.Timestamp.GoTime().Add(p.MaximumPTSAge).Before(timeutil.Now()) {
				stats.expired++
				ptsExpired := errors.Newf(
					"protected timestamp records %s as of %s (age %s) exceeds job configured limit of %s",
					rec.ID, rec.Timestamp, timeutil.Since(rec.Timestamp.GoTime()), p.MaximumPTSAge)
				log.Warningf(ctx, "job %d canceled due to %s", jobID, ptsExpired)
				return ju.CancelRequestedWithReason(ctx, md, ptsExpired)
			}
			return nil
		})
	if err != nil {
		if jobs.HasJobNotFoundError(err) {
			return nil // nolint:returnerrcheck -- job maybe deleted when we run; just keep going.
		}
		return err
	}
	return nil

}

func updateJobPTSMetrics(
	jobMetrics *jobs.Metrics, clock *hlc.Clock, ptsStats map[jobspb.Type]*ptsStat,
) {
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
				m.ProtectedAge.Update((clock.Now().WallTime - stats.oldest.WallTime) / 1e9)
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
}

func processSchedulePTSRecord(
	ctx context.Context,
	scheduleID jobspb.ScheduleID,
	rec *ptpb.Record,
	ptsStats map[string]*schedulePTSStat,
	txn isql.Txn,
) error {
	sj, err := jobs.ScheduledJobTxn(txn).
		Load(ctx, scheduledjobs.ProdJobSchedulerEnv, scheduleID)
	if err != nil {
		if jobs.HasScheduledJobNotFoundError(err) {
			return nil // nolint:returnerrcheck -- schedule maybe deleted when we run; just keep going.
		}
		return err
	}
	ex, err := jobs.GetScheduledJobExecutor(sj.ExecutorType())
	if err != nil {
		return err
	}
	pm, ok := ex.Metrics().(jobs.PTSMetrics)
	if !ok {
		return nil
	}

	stats := ptsStats[sj.ExecutorType()]
	if stats == nil {
		stats = &schedulePTSStat{m: pm.PTSMetrics()}
		ptsStats[sj.ExecutorType()] = stats
	}
	stats.numRecords++
	if stats.oldest.IsEmpty() || rec.Timestamp.Less(stats.oldest) {
		stats.oldest = rec.Timestamp
	}
	return nil
}

func updateSchedulesPTSMetrics(clock *hlc.Clock, ptsStats map[string]*schedulePTSStat) {
	for _, st := range ptsStats {
		st.m.NumWithPTS.Update(st.numRecords)
		if st.oldest.WallTime > 0 {
			st.m.PTSAge.Update((clock.Now().WallTime - st.oldest.WallTime) / 1e9)
		} else {
			st.m.PTSAge.Update(0)
		}
	}
}
