// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"maps"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// importCheckpointTracker holds all in-memory import progress that is
// periodically checkpointed to the job record. All fields are protected by
// mu so that metaFn (which records processor updates) and the checkpoint
// ticker (which reads and persists progress) always see a consistent view.
//
// In particular, SST manifests and resumePos must always advance together:
// if resumePos reflects rows up to N, the manifest buffer must contain the
// SST metadata for those rows. Without the mutex, a checkpoint could read
// an advanced resumePos but miss the corresponding manifests, causing data
// loss on resume.
type importCheckpointTracker struct {
	mu syncutil.Mutex
	// rowProgress tracks the resume position per input file.
	rowProgress []int64
	// fractionProgress tracks the completion fraction per input file.
	fractionProgress []float32
	// bulkSummary accumulates bulk operation statistics.
	bulkSummary kvpb.BulkOpSummary
	// manifestBuf accumulates SST metadata from distributed merge processors.
	// Nil when not using distributed merge.
	manifestBuf *backfill.SSTManifestBuffer

	// numFiles is the total number of input files, used to compute the
	// overall fraction completed.
	numFiles int

	// mergePhase is the completed distributed merge iteration to record.
	// When non-zero, Persist writes DistributedMergePhase to the job
	// progress. Set by SetMergeIterationResult or SetMergeComplete.
	mergePhase int32
	// clearManifests signals that the manifest job info key should be
	// cleared (written as nil). Set after the final merge iteration.
	clearManifests bool
}

func newImportCheckpointTracker(
	numFiles int, initialSummary kvpb.BulkOpSummary, manifestBuf *backfill.SSTManifestBuffer,
) *importCheckpointTracker {
	return &importCheckpointTracker{
		rowProgress:      make([]int64, numFiles),
		fractionProgress: make([]float32, numFiles),
		bulkSummary:      initialSummary,
		manifestBuf:      manifestBuf,
		numFiles:         numFiles,
	}
}

// CorrectEntryCounts replaces the entry counts in the bulk summary with the
// provided counts. This is used after distmerge to replace the inflated
// map-phase counts with the actual ingested counts from the merge processor's
// KV ingest summary, so that the next Persist writes correct row counts to
// the job progress.
func (t *importCheckpointTracker) CorrectEntryCounts(counts map[uint64]int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.bulkSummary.EntryCounts = maps.Clone(counts)
}

// RecordProcessorUpdate updates progress state from a single processor
// metadata message. The caller must provide any decoded SST manifests
// separately so that the tracker does not need to know about protobuf
// encoding details.
func (t *importCheckpointTracker) RecordProcessorUpdate(
	progress *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	manifests []jobspb.BulkSSTManifest,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(manifests) > 0 && t.manifestBuf != nil {
		t.manifestBuf.Append(manifests)
	}
	for i, v := range progress.ResumePos {
		t.rowProgress[i] = v
	}
	for i, v := range progress.CompletedFraction {
		t.fractionProgress[i] = v
	}
	t.bulkSummary.Add(progress.BulkSummary)
}

// SetMergeIterationResult replaces the manifest buffer with the output of
// a completed non-final merge iteration and records the phase number.
// The next call to Persist writes both atomically.
func (t *importCheckpointTracker) SetMergeIterationResult(
	manifests []jobspb.BulkSSTManifest, phase int32,
) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.manifestBuf = backfill.NewSSTManifestBuffer(manifests)
	t.manifestBuf.MarkDirty()
	t.mergePhase = phase
	t.clearManifests = false
}

// SetMergeComplete records the final merge iteration phase and signals
// that the manifest job info key should be cleared on the next Persist.
func (t *importCheckpointTracker) SetMergeComplete(phase int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mergePhase = phase
	t.clearManifests = true
}

// checkpointSnapshot is the data captured under the mutex for a single
// checkpoint attempt.
type checkpointSnapshot struct {
	rowProgress       []int64
	fractionProgress  []float32
	bulkSummary       kvpb.BulkOpSummary
	manifestData      []byte
	hasDirtyManifests bool
	mergePhase        int32
	clearManifests    bool
}

// snapshot captures the current progress state under the mutex and returns
// a consistent snapshot for the checkpoint transaction.
func (t *importCheckpointTracker) snapshot() (checkpointSnapshot, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	snap := checkpointSnapshot{
		rowProgress:      append([]int64(nil), t.rowProgress...),
		fractionProgress: append([]float32(nil), t.fractionProgress...),
		bulkSummary:      t.bulkSummary.DeepCopy(),
	}

	snap.mergePhase = t.mergePhase
	snap.clearManifests = t.clearManifests

	snap.hasDirtyManifests = t.manifestBuf != nil && t.manifestBuf.Dirty()
	if snap.hasDirtyManifests {
		manifests := t.manifestBuf.SnapshotAndMarkClean()
		var err error
		snap.manifestData, err = protoutil.Marshal(
			&jobspb.BulkSSTManifests{Manifests: manifests},
		)
		if err != nil {
			t.manifestBuf.MarkDirty()
			return checkpointSnapshot{}, errors.Wrap(err, "marshaling SST manifests")
		}
	}
	return snap, nil
}

// markManifestsDirty re-marks the manifest buffer dirty after a failed
// checkpoint transaction so the next attempt retries writing them.
func (t *importCheckpointTracker) markManifestsDirty() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.manifestBuf.MarkDirty()
}

// Persist snapshots the current progress under the mutex and writes it to
// the job record. If SST manifests are dirty, they are serialized and
// written to a job info key in the same transaction. On transaction
// failure, the manifest buffer is re-marked dirty so the next checkpoint
// retries.
func (t *importCheckpointTracker) Persist(ctx context.Context, job *jobs.Job) error {
	snap, err := t.snapshot()
	if err != nil {
		return err
	}

	numFiles := t.numFiles
	//lint:ignore SA1019 TODO: migrate to job_info_storage.go API
	err = job.DeprecatedDebugNameNoTxn(importProgressDebugName).Update(ctx, func(
		txn isql.Txn, md jobs.DeprecatedJobMetadata, ju *jobs.DeprecatedJobUpdater,
	) error {
		if err := md.CheckRunningOrReverting(); err != nil {
			return err
		}

		prog := md.Progress.GetImport()

		var overall float32
		copy(prog.ResumePos, snap.rowProgress)
		for i := range snap.fractionProgress {
			prog.ReadProgress[i] = snap.fractionProgress[i]
			overall += snap.fractionProgress[i]
		}
		prog.Summary = snap.bulkSummary

		if snap.clearManifests {
			if err := jobs.WriteChunkedFileToJobInfo(
				ctx, importSSTManifestsInfoKey, nil, txn, job.ID(),
			); err != nil {
				return errors.Wrap(err, "clearing SST manifests from job info")
			}
		} else if len(snap.manifestData) > 0 {
			if err := jobs.WriteChunkedFileToJobInfo(
				ctx, importSSTManifestsInfoKey, snap.manifestData, txn, job.ID(),
			); err != nil {
				return errors.Wrap(err, "writing SST manifests to job info")
			}
		}

		if snap.mergePhase > 0 {
			prog.DistributedMergePhase = snap.mergePhase
		}

		fractionCompleted := overall / float32(numFiles)
		// Clamp to [0.0, 1.0].
		if fractionCompleted > 1.0 {
			fractionCompleted = 1.0
		} else if fractionCompleted < 0.0 {
			fractionCompleted = 0
		}
		md.Progress.Progress = &jobspb.Progress_FractionCompleted{
			FractionCompleted: fractionCompleted,
		}
		ju.UpdateProgress(md.Progress)
		return nil
	})

	// If the transaction failed and we had dirty manifests, re-mark
	// the buffer dirty so the next checkpoint retries.
	if err != nil && snap.hasDirtyManifests {
		t.markManifestsDirty()
	}
	return err
}
