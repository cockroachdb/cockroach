// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ptsTargetLag is how far behind the gateway frontier we keep the
// protected timestamp record. The PTS is never advanced beyond
// `frontier - ptsTargetLag`, even if the frontier is much further
// ahead. This bounds the history GC must retain while giving the log
// writer slack to recover from transient stalls without losing data.
const ptsTargetLag = 10 * time.Minute

// ptsAdvanceThreshold is the minimum gap between the current PTS
// timestamp and the next candidate timestamp before we issue an
// UpdateTimestamp. This avoids hammering the protected timestamp
// table on every frontier advance: typical frontier movement is
// sub-second, but writing to the PTS table requires a transaction.
// One write per ~minute keeps the PTS within ~(ptsTargetLag +
// ptsAdvanceThreshold) of the frontier.
const ptsAdvanceThreshold = time.Minute

// ptsManager owns the protected-timestamp lifecycle for the revlog
// writer job. It is constructed and held by the gateway-side
// TickManager wiring (see flow.go); per-node processors never see it.
//
// Lifecycle:
//
//   - install: synchronously called once at job start. Allocates a
//     PTS record covering target at startHLC and persists the
//     record's UUID into the sibling job's BackupDetails.
//     ProtectedTimestampRecord field via Job.Update. Persisting on
//     the details is what lets the existing BACKUP OnFailOrCancel
//     find and Release the record on teardown — the sibling is a
//     regular BACKUP job (just with RevLogJob=true) and runs the
//     same backupResumer.OnFailOrCancel as any other BACKUP. We do
//     not duplicate that release logic here.
//   - advance: called from TickManager's afterFrontierAdvance hook
//     each time the aggregate frontier moves. If the gap between
//     the current record timestamp and (frontier - ptsTargetLag)
//     exceeds ptsAdvanceThreshold, we issue an UpdateTimestamp.
//     Otherwise we no-op to avoid PTS-table churn. The PTS is
//     deliberately never advanced past frontier - ptsTargetLag, so
//     the writer always has at least ptsTargetLag of slack to
//     recover from a stall before GC removes data it needed.
type ptsManager struct {
	job         *jobs.Job
	ptsProvider protectedts.Manager
	internalDB  isql.DB
	target      *ptpb.Target
	startHLC    hlc.Timestamp

	mu struct {
		syncutil.Mutex

		// recordID is the UUID assigned by install. Zero before
		// install completes; advance is a no-op until then.
		recordID uuid.UUID

		// recordTS is the timestamp currently protected by the
		// record. After install it equals startHLC; after each
		// successful UpdateTimestamp it equals the new timestamp.
		recordTS hlc.Timestamp
	}
}

// newPTSManager constructs a ptsManager. install must be called
// before advance to allocate the record.
func newPTSManager(
	job *jobs.Job,
	ptsProvider protectedts.Manager,
	internalDB isql.DB,
	target *ptpb.Target,
	startHLC hlc.Timestamp,
) *ptsManager {
	return &ptsManager{
		job:         job,
		ptsProvider: ptsProvider,
		internalDB:  internalDB,
		target:      target,
		startHLC:    startHLC,
	}
}

// install allocates a fresh PTS record at startHLC and writes its
// UUID into the sibling job's BackupDetails.ProtectedTimestampRecord
// in the same transaction that creates the record. Idempotent
// against partial failures: if the job already has a PTS UUID
// recorded (e.g. left over from a previous run that crashed after
// persisting the UUID but before continuing), install reuses it
// rather than allocating a new one — orphaning the prior UUID by
// overwriting it would defeat the OnFailOrCancel-based release path.
func (p *ptsManager) install(ctx context.Context) error {
	return p.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		details, ok := md.Payload.UnwrapDetails().(jobspb.BackupDetails)
		if !ok {
			return errors.AssertionFailedf(
				"revlogjob.ptsManager.install: unexpected payload details type %T", md.Payload.UnwrapDetails(),
			)
		}

		// If a PTS record is already recorded on the job, reuse it and
		// skip the (re-)Protect call — typically left over from a
		// previous resumption that installed it.
		pts := p.ptsProvider.WithTxn(txn)
		if details.ProtectedTimestampRecord != nil {
			rec, err := pts.GetRecord(ctx, *details.ProtectedTimestampRecord)
			if err == nil {
				p.mu.Lock()
				p.mu.recordID = *details.ProtectedTimestampRecord
				p.mu.recordTS = rec.Timestamp
				p.mu.Unlock()
				return nil
			}
			if !errors.Is(err, protectedts.ErrNotExists) {
				return errors.Wrap(err, "fetching existing PTS record")
			}
			// The record ID was on the job but the record itself
			// is gone (e.g. a previous OnFailOrCancel released it
			// but a subsequent resumption is now installing a new
			// one). Fall through and allocate a fresh record.
		}

		recordID := uuid.MakeV4()
		rec := jobsprotectedts.MakeRecord(
			recordID, int64(p.job.ID()), p.startHLC, jobsprotectedts.Jobs, p.target,
		)
		if err := pts.Protect(ctx, rec); err != nil {
			return errors.Wrap(err, "protecting timestamp")
		}

		details.ProtectedTimestampRecord = &recordID
		md.Payload.Details = jobspb.WrapPayloadDetails(details)
		ju.UpdatePayload(md.Payload)

		p.mu.Lock()
		p.mu.recordID = recordID
		p.mu.recordTS = p.startHLC
		p.mu.Unlock()

		log.Dev.Infof(ctx, "revlogjob: installed PTS %s at %s for job %d",
			recordID, p.startHLC, p.job.ID())
		return nil
	})
}

// advance considers advancing the PTS record toward
// `frontier - ptsTargetLag`. It is a no-op if:
//
//   - install has not yet completed (mu.recordID is the zero UUID),
//   - frontier is too small to project past startHLC after the
//     ptsTargetLag subtraction, or
//   - the gap between mu.recordTS and the candidate timestamp is
//     less than ptsAdvanceThreshold.
//
// On UpdateTimestamp success the in-memory recordTS is updated.
// Failures are logged but not returned: a transient PTS update
// failure does not warrant failing the writer job, and the next
// advance call will retry with a (likely larger) candidate
// timestamp.
func (p *ptsManager) advance(ctx context.Context, frontier hlc.Timestamp) {
	p.mu.Lock()
	recordID := p.mu.recordID
	current := p.mu.recordTS
	p.mu.Unlock()
	if recordID == (uuid.UUID{}) {
		return
	}

	// Candidate is frontier - ptsTargetLag. If the frontier is too
	// close to startHLC for the subtraction to land past the
	// already-protected timestamp, there's nothing useful to do.
	candidate := frontier.AddDuration(-ptsTargetLag)
	if !current.AddDuration(ptsAdvanceThreshold).Less(candidate) {
		return
	}

	if err := p.internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return p.ptsProvider.WithTxn(txn).UpdateTimestamp(ctx, recordID, candidate)
	}); err != nil {
		log.Dev.Warningf(ctx, "revlogjob: advancing PTS %s to %s: %v", recordID, candidate, err)
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Only move recordTS forward, in case a concurrent advance racing
	// with us already wrote a larger value.
	if p.mu.recordTS.Less(candidate) {
		p.mu.recordTS = candidate
	}
}
