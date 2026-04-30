// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// revlogJobDir is the directory under the BACKUP collection root
// that holds claim markers for revlog (continuous backup) sibling
// jobs. Marker files are named "<019d-wall-nanos>_<jobID>.pb" with
// empty content; the wall-time prefix gives lex-ascending = creation
// order, and the embedded jobID lets a reader identify the claimant
// without opening the file. The oldest marker whose job is
// non-terminal is the current owner of the destination's log.
const revlogJobDir = "log/job/"

// formatJobMarkerName builds a marker basename (no directory). The
// 19-digit zero-padded wall nanosecond gives lex order = chronological
// order; jobID is the tiebreaker for the (extremely unlikely) case of
// two writes within the same nanosecond.
func formatJobMarkerName(t time.Time, id jobspb.JobID) string {
	return fmt.Sprintf("%019d_%d.pb", t.UnixNano(), id)
}

// parseJobMarkerJobID extracts the jobID from a marker basename
// produced by formatJobMarkerName. Returns ok=false for names that
// don't match the expected shape.
func parseJobMarkerJobID(basename string) (jobspb.JobID, bool) {
	name := strings.TrimSuffix(basename, ".pb")
	idx := strings.IndexByte(name, '_')
	if idx < 0 || idx == len(name)-1 {
		return 0, false
	}
	id, err := strconv.ParseInt(name[idx+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return jobspb.JobID(id), true
}

// findOldestNonTerminalMarker walks log/job/ in lex order and returns
// the basename and parsed jobID of the oldest marker whose job is
// non-terminal. Markers pointing at jobs that don't exist (orphans
// from a parent BACKUP that crashed mid-claim, or our own marker in
// phase 3 before we've created the job) are treated as terminal
// unless their jobID equals selfID — that case represents the caller
// racing for the claim and is reported as live. Returns ("", 0, nil)
// if no non-terminal claim exists. Pass selfID=0 to skip the
// self-aliveness shortcut (phase 1 of the create dance).
//
// TODO(dt): on terminate, the writer should best-effort delete its
// own marker and, on delete failure (WORM), drop a sibling
// "<basename>.terminated" so this walk can short-circuit a registry
// lookup in the WORM case. Skipped for now to keep the LIST size to
// one entry per generation.
func findOldestNonTerminalMarker(
	ctx context.Context, store cloud.ExternalStorage, registry *jobs.Registry, selfID jobspb.JobID,
) (string, jobspb.JobID, error) {
	var names []string
	if err := store.List(ctx, revlogJobDir, cloud.ListOptions{}, func(name string) error {
		names = append(names, name)
		return nil
	}); err != nil {
		return "", 0, errors.Wrap(err, "listing revlog job markers")
	}
	// ExternalStorage.List documents undefined ordering; sort
	// defensively so phase 1 and phase 3 see the same view.
	sort.Strings(names)
	for _, name := range names {
		id, ok := parseJobMarkerJobID(name)
		if !ok {
			continue
		}
		if id == selfID {
			return name, id, nil
		}
		j, err := registry.LoadJob(ctx, id)
		if err != nil {
			if jobs.HasJobNotFoundError(err) {
				continue
			}
			return "", 0, errors.Wrapf(err, "loading revlog claimant job %d", id)
		}
		if j.State().Terminal() {
			continue
		}
		return name, id, nil
	}
	return "", 0, nil
}

// maybeCreateRevlogSiblingJob performs the create-or-noop dance for
// the sibling revlog (continuous backup) job described in the
// continuous backup RFC's "Job creation" subsection. It is meant to
// be called from the BACKUP resumer's normal execution path when the
// `CreateRevlogJob` flag is set on the BACKUP's details.
//
// Algorithm:
//
//  1. LIST log/job/ and walk in ascending order. If the oldest marker
//     whose job is non-terminal exists, the destination already has
//     an active log job — silently no-op.
//  2. Mint a fresh jobID and write an empty marker file at
//     log/job/<wall-nanos>_<jobID>.pb. The file is empty: the jobID
//     is encoded into the name so a reader doesn't need to open it.
//  3. LIST again. If our marker is the oldest non-terminal entry, we
//     won the race — create the adoptable sibling job with the
//     pre-minted ID. Otherwise an earlier writer beat us; best-effort
//     delete our marker (delete may fail on WORM and is tolerated)
//     and silently no-op without creating a job.
//
// Creating the job after the win in step 3 means a lost race leaves
// no orphaned job — only (possibly) an empty marker file.
//
// The store argument must be rooted at the BACKUP's *collection*
// URI (details.CollectionURI), not at the per-backup details.URI:
// markers live at the collection root alongside log/ so future
// BACKUPs into the same collection find them via step 1, regardless
// of which timestamped backup directory they end up writing to.
func maybeCreateRevlogSiblingJob(
	ctx context.Context,
	store cloud.ExternalStorage,
	parentDetails jobspb.BackupDetails,
	parentDescription string,
	user username.SQLUsername,
	jobRegistry *jobs.Registry,
	db isql.DB,
) error {
	// Phase 1: speculative check. If a non-terminal claim exists, do
	// nothing — no marker written, no job created.
	if _, ownerID, err := findOldestNonTerminalMarker(ctx, store, jobRegistry, 0); err != nil {
		return err
	} else if ownerID != 0 {
		log.Dev.Infof(ctx, "revlog job %d already owns destination; skipping sibling creation", ownerID)
		return nil
	}

	// Phase 2: optimistic write. Mint our jobID and drop our marker.
	// The job itself is not created yet — we don't want to leave an
	// orphan if we lose phase 3.
	siblingJobID := jobRegistry.MakeJobID()
	markerName := revlogJobDir + formatJobMarkerName(timeutil.Now(), siblingJobID)
	if err := cloud.WriteFile(ctx, store, markerName, bytes.NewReader(nil)); err != nil {
		return errors.Wrapf(err, "writing revlog job marker %q", markerName)
	}

	// Phase 3: prove. The oldest non-terminal marker is the winner. If
	// it's not us, an earlier writer beat us; clean up and bail.
	winnerName, winnerID, err := findOldestNonTerminalMarker(ctx, store, jobRegistry, siblingJobID)
	if err != nil {
		return err
	}
	if winnerID != siblingJobID {
		// Best-effort delete. On WORM buckets delete may fail; the
		// stale empty marker is harmless (future readers will look up
		// our jobID, find no job, and treat it as terminal).
		if delErr := store.Delete(ctx, markerName); delErr != nil {
			log.Dev.Warningf(ctx, "failed to delete losing revlog marker %q: %v", markerName, delErr)
		}
		log.Dev.Infof(ctx,
			"lost revlog claim race to job %d (marker %q); skipping sibling creation",
			winnerID, winnerName)
		return nil
	}

	// We won. Build and create the sibling BACKUP job.
	siblingDetails := parentDetails
	siblingDetails.CreateRevlogJob = false
	siblingDetails.RevLogJob = true
	// CRITICAL: clear the parent's PTS record from the sibling's
	// details. Otherwise the sibling's OnFailOrCancel would Release
	// the parent BACKUP's PTS by ID — orphaning the parent and
	// possibly letting GC remove data the parent still needs. The
	// sibling installs its own PTS during its own resumer execution.
	siblingDetails.ProtectedTimestampRecord = nil

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
