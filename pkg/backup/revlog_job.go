// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// revlogJobMarkerPrefix is the prefix under which a destination's
// "log job already exists" marker file lives. The full file name is
// `revlogJobMarkerPrefix + revlogJobMarkerSuffix` (currently a fixed
// "-1" suffix; see TODO below). LIST(prefix=revlogJobMarkerPrefix,
// limit=1) returning any file means a log job has already been
// established for this destination, and a fresh BACKUP with
// `WITH REVISION STREAM` should no-op the create-or-noop dance.
//
// TODO(dt): replace the constant "-1" suffix with a descending
// `now()` encoding so multiple generations of log jobs over the
// destination's lifetime are findable in chronological-newest-first
// order; on existence-check, read the latest file, parse the
// embedded job ID, and check its status (running / paused /
// canceled / failed) — like a pidfile. See the "Job creation"
// subsection of docs/RFCS/20260420_continuous_backup.md.
const (
	revlogJobMarkerPrefix = "log/job.latest-"
	revlogJobMarkerSuffix = "1"
	revlogJobMarkerName   = revlogJobMarkerPrefix + revlogJobMarkerSuffix
)

// maybeCreateRevlogSiblingJob performs the create-or-noop dance for
// the sibling revlog (continuous backup) job described in the
// continuous backup RFC's "Job creation" subsection. It is meant to
// be called from the BACKUP resumer's normal execution path when the
// `CreateRevlogJob` flag is set on the BACKUP's details.
//
// Algorithm:
//
//  1. LIST the destination for any object whose name starts with
//     `log/job.latest-`. If one exists, a log job has already been
//     established for this destination, so the dance no-ops. This
//     gives idempotency across scheduled BACKUPs.
//  2. Otherwise, create a sibling BACKUP job whose details are a copy
//     of the parent BACKUP's details with `CreateRevlogJob=false` and
//     `RevLogJob=true`. The flag at the top of the BACKUP resumer
//     (see Resume) keys off `RevLogJob` to dispatch to the revlog
//     execution path.
//  3. Persist the new sibling job's ID into `log/job.latest-1` on
//     the destination so subsequent BACKUPs find it via the LIST in
//     step 1.
//
// The store argument must be rooted at the BACKUP's *collection*
// URI (details.CollectionURI), not at the per-backup details.URI:
// the marker lives at the collection root alongside log/ so future
// BACKUPs into the same collection find it via the existence-check
// LIST in step 1, regardless of which timestamped backup directory
// they end up writing to.
//
// In v1 a small race exists if two BACKUPs concurrently see no
// marker and both create log jobs: both PUTs to `log/job.latest-1`
// succeed (object stores allow PUT-overwrite by default). This is
// accepted for v1 — see the RFC's "Job creation" subsection.
func maybeCreateRevlogSiblingJob(
	ctx context.Context,
	store cloud.ExternalStorage,
	parentDetails jobspb.BackupDetails,
	parentDescription string,
	user username.SQLUsername,
	jobRegistry *jobs.Registry,
	db isql.DB,
) error {
	// Step 1: existence check. We use ErrListingDone to short-circuit
	// after the first match; ExternalStorage.List has no native limit.
	exists, err := revlogJobMarkerExists(ctx, store)
	if err != nil {
		return errors.Wrap(err, "checking for existing revlog job marker")
	}
	if exists {
		log.Dev.Infof(ctx, "revlog job marker already present for destination; skipping sibling job creation")
		return nil
	}

	// Step 2: build the sibling BACKUP job's record. We construct a
	// fresh BackupDetails populated with only the fields the revlog
	// code path actually reads, rather than cloning the parent's
	// details and trying to nil out the dangerous bits.
	//
	// Cloning is unsafe: backupResumer.OnFailOrCancel runs for both
	// the parent and the sibling (they share the resumer), and it
	// uses several BackupDetails fields to drive cleanup —
	// details.URI for deleteCheckpoint, details.ProtectedTimestampRecord
	// for releaseProtectedTimestamp, details.ScheduleID for schedule
	// notifications, details.UpdatesClusterMonitoringMetrics for
	// metric updates. A cloned sibling pointed at any of those
	// would, on cancel/failure, clobber the parent's checkpoint dir,
	// release the parent's PTS, mis-notify the parent's schedule, or
	// poison the parent's metric tile.
	//
	// Allowlist instead. New BackupDetails fields default to "not in
	// the revlog sibling" — safe by construction. The cost is that
	// adding a field the revlog *does* need requires updating this
	// list, but that bug surfaces immediately as a missing-feature
	// failure, whereas the cloning bugs are silent until something
	// fails.
	siblingDetails := jobspb.BackupDetails{
		// Dispatch flag: backupResumer.Resume keys off this to take
		// the revlog fork.
		RevLogJob: true,

		// Destination: revlog writes under CollectionURI/log/. The
		// per-run URI/URIsByLocalityKV are deliberately omitted —
		// they describe the parent's per-run data destination and
		// are what deleteCheckpoint would clobber.
		CollectionURI: parentDetails.CollectionURI,

		// startHLC for the rangefeed (revlogjob.Run reads this as
		// the rangefeed subscription start).
		EndTime: parentDetails.EndTime,

		// Scope inputs: revlogScope closes over these to decide
		// what's in-scope by identity.
		FullCluster:                parentDetails.FullCluster,
		ResolvedTargets:            parentDetails.ResolvedTargets,
		ResolvedCompleteDbs:        parentDetails.ResolvedCompleteDbs,
		SpecificTenantIds:          parentDetails.SpecificTenantIds,
		IncludeAllSecondaryTenants: parentDetails.IncludeAllSecondaryTenants,

		// ProtectedTimestampRecord intentionally left zero. The
		// sibling installs its own PTS during its resumer execution
		// (v1 self-managed PTS, see pkg/revlog/revlogjob/pts.go).
		// Inheriting the parent's would be catastrophic: the
		// sibling's OnFailOrCancel would Release the parent's PTS
		// by ID, orphaning the parent and letting GC remove data it
		// still needs.

		// ScheduleID + SchedulePTSChainingRecord intentionally
		// omitted: the sibling is not part of the parent's schedule
		// chain. Inheriting these would cause the sibling's
		// terminal state to mis-notify the parent's schedule.

		// UpdatesClusterMonitoringMetrics intentionally omitted:
		// the sibling's success/failure should not write to the
		// parent's monitoring tile.

		// EncryptionOptions / EncryptionInfo intentionally omitted
		// today (revlog v1 doesn't honor encryption).
		// TODO(dt): populate these when adding encryption support so
		// the revlog writes encrypted SSTs when the parent's
		// destination is encrypted; required before BACKUP ... WITH
		// ENCRYPTION ... REVISION STREAM is supported.
	}

	siblingJobID := jobRegistry.MakeJobID()
	siblingRecord := jobs.Record{
		Description: "REVLOG: " + parentDescription,
		Details:     siblingDetails,
		Progress:    jobspb.BackupProgress{},
		Username:    user,
	}

	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := jobRegistry.CreateAdoptableJobWithTxn(ctx, siblingRecord, siblingJobID, txn)
		return err
	}); err != nil {
		return errors.Wrap(err, "creating revlog sibling job")
	}

	// Step 3: persist the new job's ID into the marker file so future
	// BACKUPs find it on the existence check above.
	idStr := strconv.FormatInt(int64(siblingJobID), 10)
	if err := cloud.WriteFile(ctx, store, revlogJobMarkerName, bytes.NewReader([]byte(idStr))); err != nil {
		return errors.Wrapf(err, "writing revlog job marker %q", revlogJobMarkerName)
	}

	log.Dev.Infof(ctx, "created revlog sibling job %d for destination", siblingJobID)
	return nil
}

// defaultRevlogTickWidth is the working tick width for the v1 revlog
// writer. See revlog-format.md §2.
//
// TODO(dt): expose as a cluster setting / per-job knob.
const defaultRevlogTickWidth = 10 * time.Second

// runRevlogJob is the BACKUP-resumer hook for the sibling revlog
// (continuous backup) job. It derives the producer's span resolver,
// startHLC, and destination from the parent BACKUP's details and
// hands them off to revlogjob.Run, which builds the DistSQL flow
// (one producer per node, gateway-side TickManager writing manifests
// from collected ProducerMetadata).
//
// v1 simplifications:
//
//   - The span resolver returns the entire tenant keyspace on every
//     call (no descriptor-driven filtering). TODO(dt): consult
//     the parent's ResolvedTargets and current descriptor state so
//     the log only covers what the parent backed up. This is the
//     intended seam — the resolver can close over BackupDetails and
//     a descriptor-fetching helper, and use the changedDescIDs hint
//     once the coordinator's descriptor rangefeed is in place
//     (RFC §1 "Span coverage tracking").
//   - tickWidth is hardcoded.
//   - no resumption / persisted progress: a restarted sibling job
//     re-launches the rangefeed at startHLC and re-flushes from
//     there. The format is idempotent on file_id so duplicate
//     output is safe.
func runRevlogJob(
	ctx context.Context, p sql.JobExecContext, jobID jobspb.JobID, details jobspb.BackupDetails,
) error {
	dest := details.CollectionURI
	if dest == "" {
		return errors.AssertionFailedf("revlog job must have a CollectionURI")
	}
	if len(details.SpecificTenantIds) > 0 || details.IncludeAllSecondaryTenants {
		// TODO(dt): support tenant rev logs.
		return errors.AssertionFailedf("revlog job does not support SpecificTenantIds")
	}

	log.Dev.Infof(ctx, "starting revlog job %d at %s, dest=%s", jobID, details.EndTime, dest)

	return revlogjob.Run(ctx, p, jobID, makeRevlogScope(p.ExecCfg(), details),
		details.EndTime, dest, defaultRevlogTickWidth, ptsTargetForRevlogJob(p.ExecCfg().Codec))
}

// ptsTargetForRevlogJob returns the protected-timestamp target the
// revlog writer's self-managed PTS record should cover. v1 paints
// the entire keyspace the writer reads — for the system tenant a
// cluster-wide target, for an application tenant a single-tenant
// target — matching makeTenantSpanResolver above.
//
// TODO(dt): once makeTenantSpanResolver narrows to descriptor-
// scoped spans, narrow this target to MakeSchemaObjectsTarget over
// the same descriptor set so GC isn't pinned cluster-wide.
func ptsTargetForRevlogJob(codec keys.SQLCodec) *ptpb.Target {
	if codec.ForSystemTenant() {
		return ptpb.MakeClusterTarget()
	}
	return ptpb.MakeTenantsTarget([]roachpb.TenantID{codec.TenantID})
}

// makeRevlogScope builds the revlogjob.Scope the writer uses to
// answer "what spans should I currently cover?" / "does this
// descriptor change matter?" / "have all my roots gone away?".
// The implementation closes over the parent BACKUP's
// already-resolved targets so the log mirrors the chain it
// serves; see revlogScope.
func makeRevlogScope(execCfg *sql.ExecutorConfig, details jobspb.BackupDetails) revlogjob.Scope {
	return newRevlogScope(execCfg, details)
}

// revlogJobMarkerExists returns true if the destination already has
// at least one `log/job.latest-*` marker file. It LISTs at most one
// entry under the marker prefix and short-circuits via
// cloud.ErrListingDone.
func revlogJobMarkerExists(ctx context.Context, store cloud.ExternalStorage) (bool, error) {
	var found bool
	err := store.List(ctx, revlogJobMarkerPrefix, cloud.ListOptions{}, func(string) error {
		found = true
		return cloud.ErrListingDone
	})
	if err != nil && !errors.Is(err, cloud.ErrListingDone) {
		return false, err
	}
	return found, nil
}
