// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
)

// Run executes the revlog writer for one job: opens a rangefeed over
// the spans returned by resolveSpans, hands its events to a Producer
// which flushes data files and emits per-flush progress, and routes
// that progress through a TickManager that seals ticks and writes
// manifests.
//
// resolveSpans is the seam for backup-side scope logic: Run calls it
// for the initial span set and (eventually) re-invokes it on schema
// changes to track coverage as objects are added or removed.
//
// ptsTarget describes the keyspace covered by the writer's
// self-managed protected timestamp record (see pts.go). The caller
// constructs the target to match resolveSpans coverage; revlogjob
// does not derive one because codec/tenant context lives outside
// this package.
//
// TODO(dt): make this a real DistSQL flow. The on-the-wire shape
// (per-flush BulkProcessorProgress entries with a marshaled
// revlogpb.Flush in ProgressDetails) is preserved deliberately so
// that swap is mechanical: the producer side becomes a per-node
// processor, the channel becomes the DistSQL metadata stream, and
// the coordinator side becomes the gateway-side metadata callback.
// For now everything runs locally on the gateway in two ctxgroup
// tasks (producer + coordinator) plus a progress-updater task.
func Run(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	resolveSpans DescSpanResolver,
	startHLC hlc.Timestamp,
	dest string,
	tickWidth time.Duration,
	ptsTarget *ptpb.Target,
) error {
	if resolveSpans == nil {
		return errors.AssertionFailedf("revlogjob.Run: resolveSpans must be non-nil")
	}
	// TODO(dt): re-invoke resolveSpans on descriptor changes via a
	// descriptor-rangefeed-driven coordinator and feed coverage
	// updates into the running flow.
	spans, err := resolveSpans(ctx, startHLC, nil /* changedDescIDs */)
	if err != nil {
		return errors.Wrap(err, "resolving initial span set")
	}
	if len(spans) == 0 {
		return errors.AssertionFailedf("revlogjob.Run: resolveSpans returned no spans")
	}

	cfg := execCtx.ExecCfg()
	parsedDest, err := cloud.ExternalStorageConfFromURI(dest, username.RootUserName())
	if err != nil {
		return errors.Wrap(err, "parsing destination URI")
	}
	es, err := cfg.DistSQLSrv.ExternalStorage(ctx, parsedDest)
	if err != nil {
		return errors.Wrap(err, "opening destination storage")
	}
	defer es.Close()

	// TODO(dt): persist progress and resume mid-stream rather than
	// starting fresh from startHLC each time the resumer runs.
	manager, err := NewTickManager(es, spans, startHLC, tickWidth)
	if err != nil {
		return err
	}

	// Load the job record so the progress-updater task can call
	// Job.Update to publish HighWater. A failure to load is fatal —
	// without a job row to update, the writer would run blind to
	// operators and resume.
	job, err := cfg.JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		return errors.Wrapf(err, "loading revlog job %d", jobID)
	}

	// Install the self-managed PTS record before any writer-side
	// work begins, so we never run with the rangefeed open and
	// nothing protecting the data we're about to read. The record's
	// UUID is persisted onto the sibling job's BackupDetails by
	// install — the existing BACKUP OnFailOrCancel uses that field
	// to release the record on teardown, so we deliberately do not
	// add a release call in this package. See pts.go.
	pts := newPTSManager(
		job, cfg.ProtectedTimestampProvider,
		cfg.InternalDB, ptsTarget, startHLC,
	)
	if err := pts.install(ctx); err != nil {
		return errors.Wrap(err, "installing protected timestamp")
	}
	manager.SetAfterFrontierAdvance(pts.advance)

	// progCh carries per-flush BulkProcessorProgress entries from the
	// producer task to the coordinator task. The shape matches what
	// a future DistSQL split would carry as ProducerMetadata —
	// dropping the wire encoding step is the only difference.
	progCh := make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress, 16)

	sink := newMetaSink(progCh)
	producer, err := NewProducer(es, spans, startHLC, tickWidth,
		nodeFileIDs{instanceID: unique.ProcessUniqueID(cfg.JobRegistry.ID())}, sink)
	if err != nil {
		return errors.Wrap(err, "constructing producer")
	}

	g := ctxgroup.WithContext(ctx)

	// Producer task: opens the rangefeed and pumps its events into
	// the producer. Closes progCh on exit so the coordinator task
	// drains and returns cleanly.
	g.GoCtx(func(ctx context.Context) error {
		defer close(progCh)
		eventsCh := make(chan rangefeedEvent, 1024)
		errCh := make(chan error, 1)

		rf, err := startRangeFeed(ctx, cfg.RangeFeedFactory, "revlog",
			spans, startHLC, eventsCh, errCh)
		if err != nil {
			return err
		}
		defer rf.Close()

		// runDispatcher is the loop pumping rangefeed events into the
		// producer; it returns when ctx is done or eventsCh closes.
		// We race it against the rangefeed's internal-error channel.
		dispErr := make(chan error, 1)
		go func() { dispErr <- runDispatcher(ctx, producer, eventsCh) }()

		// TODO(dt): collect more than the first error from errCh and
		// either surface them all or retry per WithRetryStrategy.
		select {
		case err := <-dispErr:
			return err
		case err := <-errCh:
			return errors.Wrap(err, "rangefeed internal error")
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Coordinator task: drains progCh, wraps each entry as a
	// ProducerMetadata (the shape a future DistSQL processor would
	// surface from its Next), and applies it to the manager.
	g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case prog, ok := <-progCh:
				if !ok {
					return nil
				}
				meta := &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
				if err := handleProducerMetadata(ctx, manager, meta); err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Progress-updater task: publishes manager.LastClosed() to the
	// job's HighWater on a timer. Reads only; safe to run alongside
	// the coordinator's writes to the manager.
	g.GoCtx(func(ctx context.Context) error {
		return runProgressUpdater(ctx, job, manager)
	})

	return g.Wait()
}

// handleProducerMetadata routes one ProducerMetadata into the
// manager. Non-progress metadata (e.g. tracing) is ignored. Kept in
// terms of execinfrapb.ProducerMetadata so the future DistSQL split
// can plug it directly into the gateway-side metadata callback.
func handleProducerMetadata(
	ctx context.Context, manager *TickManager, meta *execinfrapb.ProducerMetadata,
) error {
	if meta == nil || meta.BulkProcessorProgress == nil {
		return nil
	}
	flush, err := DecodeFlush(meta.BulkProcessorProgress.ProgressDetails)
	if err != nil {
		return err
	}
	return manager.Flush(ctx, flush)
}

// nodeFileIDs is a FileIDSource backed by the cluster's unique-int
// generator. The IDs are non-negative (top bit always zero), roughly
// monotonic per node (timestamp-derived), and disambiguated across
// nodes by the embedded SQL instance ID — same scheme backup uses
// for its data SST names (pkg/backup/backupsink/sink_utils.go).
type nodeFileIDs struct {
	instanceID unique.ProcessUniqueID
}

func (n nodeFileIDs) Next() int64 {
	return unique.GenerateUniqueInt(n.instanceID)
}
