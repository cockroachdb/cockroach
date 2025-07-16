// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// BackupRestoreTestingKnobs contains knobs for backup and restore behavior.
type BackupRestoreTestingKnobs struct {
	// AfterBackupChunk is called after each chunk of a backup is completed.
	AfterBackupChunk func()

	// AfterBackupCheckpoint if set will be called after a BACKUP-CHECKPOINT
	// is written.
	AfterBackupCheckpoint func()

	// AfterLoadingCompactionManifestOnResume is run once the backup manifest has been
	// loaded/created on the resumption of a compaction job.
	AfterLoadingCompactionManifestOnResume func(manifest *backuppb.BackupManifest)

	// CaptureResolvedTableDescSpans allows for intercepting the spans which are
	// resolved during backup planning, and will eventually be backed up during
	// execution.
	CaptureResolvedTableDescSpans func([]roachpb.Span)

	// RunAfterSplitAndScatteringEntry allows blocking the RESTORE job after a
	// single RestoreSpanEntry has been split and scattered.
	RunAfterSplitAndScatteringEntry func(ctx context.Context)

	// RunAfterProcessingRestoreSpanEntry allows blocking the RESTORE job after a
	// single RestoreSpanEntry has been processed and added to the SSTBatcher.
	RunAfterProcessingRestoreSpanEntry func(ctx context.Context, entry *execinfrapb.RestoreSpanEntry) error

	// RunAfterExportingSpanEntry allows blocking the BACKUP job after a single
	// span has been exported.
	RunAfterExportingSpanEntry func(ctx context.Context, response *kvpb.ExportResponse)

	// BackupMonitor is used to overwrite the monitor used by backup during
	// testing. This is typically the bulk mem monitor if not
	// specified here.
	BackupMemMonitor *mon.BytesMonitor

	RestoreDistSQLRetryPolicy *retry.Options

	// RestoreRetryProgressThreshold allows configuring the threshold at which
	// the restore will no longer fast fail after a certain number of retries.
	RestoreRetryProgressThreshold float32

	RunBeforeRestoreFlow func() error

	RunAfterRestoreFlow func() error

	BackupDistSQLRetryPolicy *retry.Options

	RunBeforeBackupFlow func() error

	RunAfterBackupFlow func() error

	RunAfterRetryIteration func(err error) error

	RunAfterRestoreProcDrains func()

	RunBeforeResolvingCompactionDest func() error

	// RunBeforeSendingDownloadSpan is called within the retry loop of the
	// download span worker before sending the download span request.
	RunBeforeSendingDownloadSpan func() error

	// RunBeforeDownloadCleanup is called before we cleanup after all external
	// files have been download.
	RunBeforeDownloadCleanup func() error
}

var _ base.ModuleTestingKnobs = &BackupRestoreTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*BackupRestoreTestingKnobs) ModuleTestingKnobs() {}
