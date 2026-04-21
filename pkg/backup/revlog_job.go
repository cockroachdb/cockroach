// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"
	"math"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

	// Step 2: build the sibling BACKUP job's record. We copy the
	// parent's details so the sibling shares destination, encryption,
	// etc., then flip the flags so the resumer takes the revlog fork.
	siblingDetails := parentDetails
	siblingDetails.CreateRevlogJob = false
	siblingDetails.RevLogJob = true
	// CRITICAL: clear the parent's PTS record from the sibling's
	// details. Otherwise the sibling's OnFailOrCancel would Release
	// the parent BACKUP's PTS by ID — orphaning the parent and
	// possibly letting GC remove data the parent still needs. The
	// sibling will install its own PTS during its own resumer
	// execution (per the v1 self-managed PTS design in the RFC's
	// "PTS — v1 self-managed; future cluster-coordinated" concern).
	siblingDetails.ProtectedTimestampRecord = nil

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

	return revlogjob.Run(ctx, p, jobID, makeDescSpanResolver(p.ExecCfg().Codec, details),
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

// makeTenantSpanResolver returns a revlogjob.DescSpanResolver, which determines
// if the revlogjob should watch new spans given the changed descroptor IDs.
//
// TODO(dt): currently this just returns the whole span of all possible tables;
// this works as a placeholder for v0 since it should include this should be
// updated to be the actual targets the backup is backing up, but should be
// replaced with something that returns spansForAllTableIndexes based on what
// the re-resolved targets.
func makeDescSpanResolver(
	codec keys.SQLCodec, details jobspb.BackupDetails,
) revlogjob.DescSpanResolver {
	return func(
		ctx context.Context, asOf hlc.Timestamp, changedDescs []catid.DescID,
	) ([]roachpb.Span, error) {
		return []roachpb.Span{{
			Key:    codec.TablePrefix(0),
			EndKey: codec.TablePrefix(math.MaxUint32).PrefixEnd(),
		}}, nil
	}
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
