// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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

// runRevlogJob is the BACKUP-resumer hook for the sibling revlog
// (continuous backup) job. The actual revlog writer lives in
// pkg/revlog/revlogjob and will be wired in by a follow-up commit;
// this stub returns an unimplemented error so the syntax + sibling-
// job machinery here can land independently of the writer.
func runRevlogJob(
	_ context.Context, _ sql.JobExecContext, _ jobspb.JobID, _ jobspb.BackupDetails,
) error {
	return errors.New("BACKUP ... WITH REVISION STREAM is not yet implemented")
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
