// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// maybeAdjustEndTimeForRevisionLog checks whether the collection has a revision
// log and, if so, adjusts the restore end time to the latest backup in
// the specified chain whose end time is at or before endTime. The
// original endTime is returned as the revision log replay target so that
// the caller can replay log entries from the backup's end through the
// requested AOST.
//
// fullSubdir identifies the backup chain (e.g. "2026/04/20-150405.00")
// so that only backups from that chain are considered.
//
// The end time is returned unchanged (with an empty revlog timestamp)
// when any of the following are true:
//   - this is a release build (revlog restore is prototype-only)
//   - no revision log exists at the collection root
//   - a backup in the chain exactly matches endTime
func maybeAdjustEndTimeForRevisionLog(
	ctx context.Context, store cloud.ExternalStorage, endTime hlc.Timestamp, fullSubdir string,
) (adjustedEndTime, revlogReplayTarget hlc.Timestamp, _ error) {
	if build.IsRelease() {
		return endTime, hlc.Timestamp{}, nil
	}

	hasLog, err := revlog.HasLog(ctx, store)
	if err != nil {
		return endTime, hlc.Timestamp{}, err
	}
	if !hasLog {
		return endTime, hlc.Timestamp{}, nil
	}

	// Find the latest backup whose end time is at or before endTime.
	// The backups returned by ListRestorableBackups may belong to a different
	// subdir or be slightly newer than endTime, so we list enough to ensure we
	// find a match.
	// TODO (kev-cao): This is slightly awkward, but I think the introduction of
	// revision log restore somewhat changes the semantics of the restore command,
	// which is a separate discussion.
	backups, _, err := backupinfo.ListRestorableBackups(
		ctx, store,
		time.Time{},                        /* newerThan */
		timeutil.Unix(0, endTime.WallTime), /* olderThan */
		4,                                  /* maxCount */
		true,                               /* openIndex */
	)
	if err != nil {
		return endTime, hlc.Timestamp{},
			errors.Wrap(err, "finding backup for revision log restore")
	}
	for _, b := range backups {
		if b.FullSubdir != fullSubdir || endTime.Less(b.EndTime) {
			continue
		}
		if endTime.Equal(b.EndTime) {
			// The AOST matches this backup exactly; a normal
			// restore is sufficient and no log replay is needed.
			return endTime, hlc.Timestamp{}, nil
		}
		return b.EndTime, endTime, nil
	}
	return endTime, hlc.Timestamp{},
		errors.New("no backup found with end time at or before the specified AS OF SYSTEM TIME")
}

// restoreFromRevisionLog replays revision log entries from the backup's end
// time through the target AOST timestamp, ingesting the mutations on top of the
// already-restored backup data.
func (r *restoreResumer) restoreFromRevisionLog(ctx context.Context) error {
	return nil // no-op stub
}
