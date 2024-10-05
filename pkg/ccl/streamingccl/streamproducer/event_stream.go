// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamproducer

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var activeStreams = struct {
	syncutil.Mutex
	m map[*eventStream]*eventStream
}{m: make(map[*eventStream]*eventStream)}

type eventStream struct {
	streamID streampb.StreamID
	execCfg  *sql.ExecutorConfig
	spec     streampb.StreamPartitionSpec
	frontier span.Frontier

	// streamCh and data are used to pass rows back to be emitted to the caller.
	streamCh chan tree.Datums
	errCh    chan error
	data     tree.Datums

	// Fields below initialized when Start called.
	rf  *rangefeed.RangeFeed
	mon *mon.BytesMonitor
	acc mon.BoundAccount

	// The remaining fields are used to process rangefeed messages.
	// addMu is non-nil during initial scans, where it serializes the onValue and
	// checkpoint calls that initial scans make from its parallel scan workers; it
	// is set to nil after initial scan since rangefeed says all other calls are
	// done serially from the event loop worker.
	addMu              *syncutil.Mutex
	seb                streamEventBatcher
	lastCheckpointTime time.Time
	lastCheckpointLen  int

	debug streampb.DebugProducerStatus
}

var quantize = settings.RegisterDurationSettingWithExplicitUnit(
	settings.SystemOnly,
	"physical_replication.producer.timestamp_granularity",
	"the granularity at which replicated times are quantized to make tracking more efficient",
	5*time.Second,
)

var emitMetadata = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"physical_replication.producer.emit_metadata.enabled",
	"whether to emit metadata events",
	true,
)

var _ eval.ValueGenerator = (*eventStream)(nil)

var eventStreamReturnType = types.MakeLabeledTuple(
	[]*types.T{types.Bytes},
	[]string{"stream_event"},
)

// ResolvedType implements eval.ValueGenerator interface.
func (s *eventStream) ResolvedType() *types.T {
	return eventStreamReturnType
}

// Start implements eval.ValueGenerator interface.
func (s *eventStream) Start(ctx context.Context, txn *kv.Txn) (retErr error) {
	// ValueGenerator API indicates that Start maybe called again if Next returned
	// false.  However, this generator never terminates without an error,
	// so this method should be called once.  Be defensive and return an error
	// if this method is called again.
	if s.errCh != nil {
		return errors.AssertionFailedf("expected to be started once")
	}

	sourceTenantID, err := s.validateProducerJobAndSpec(ctx)
	if err != nil {
		return err
	}

	log.Infof(ctx, "starting physical replication event stream: tenant=%s initial_scan_timestamp=%s previous_replicated_time=%s",
		sourceTenantID, s.spec.InitialScanTimestamp, s.spec.PreviousReplicatedTimestamp)

	s.acc = s.mon.MakeBoundAccount()

	// errCh is buffered to ensure the sender can send an error to
	// the buffer, without waiting, when the channel receiver is not waiting on
	// the channel.
	s.errCh = make(chan error, 1)

	// Stream channel receives datums to be sent to the consumer.
	s.streamCh = make(chan tree.Datums)

	// Common rangefeed options.
	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("job", fmt.Sprintf("id=%d", s.streamID)),
		rangefeed.WithMemoryMonitor(s.mon),
		rangefeed.WithFrontierSpanVisitor(s.maybeCheckpoint),
		rangefeed.WithOnFrontierAdvance(s.onFrontier),
		rangefeed.WithOnCheckpoint(s.onCheckpoint),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) {
			s.setErr(err)
		}),
		rangefeed.WithOnSSTable(s.onSSTable),
		rangefeed.WithOnDeleteRange(s.onDeleteRange),
		rangefeed.WithFrontierQuantized(quantize.Get(&s.execCfg.Settings.SV)),
		rangefeed.WithOnValues(s.onValues),
	}
	if emitMetadata.Get(&s.execCfg.Settings.SV) {
		opts = append(opts, rangefeed.WithOnMetadata(s.onMetadata))
	}

	initialTimestamp := s.spec.InitialScanTimestamp
	s.frontier, err = span.MakeFrontier(s.spec.Spans...)
	if err != nil {
		return err
	}
	for _, sp := range s.spec.Progress {
		if _, err := s.frontier.Forward(sp.Span, sp.Timestamp); err != nil {
			s.frontier.Release()
			return err
		}
	}
	if s.spec.PreviousReplicatedTimestamp.IsEmpty() {
		s.addMu = &syncutil.Mutex{}
		log.Infof(ctx, "starting event stream with initial scan at %s", initialTimestamp)
		opts = append(opts,
			rangefeed.WithInitialScan(s.onInitialScanDone),
			rangefeed.WithRowTimestampInInitialScan(true),
			rangefeed.WithInitialScanParallelismFn(func() int {
				return int(s.spec.Config.InitialScanParallelism)
			}),
		)
	} else {
		initialTimestamp = s.spec.PreviousReplicatedTimestamp
		// When resuming from cursor, advance frontier to the cursor position.
		log.Infof(ctx, "resuming event stream (no initial scan) from %s", initialTimestamp)
	}

	// Reserve batch kvsSize bytes from monitor.  We might have to do something more fancy
	// in the future, but for now, grabbing chunk of memory from the monitor would do the trick.
	if err := s.acc.Grow(ctx, s.spec.Config.BatchByteSize); err != nil {
		return errors.Wrapf(err, "failed to allocated %d bytes from monitor", s.spec.Config.BatchByteSize)
	}

	// Start rangefeed, which spins up a separate go routine to perform its job.
	s.rf = s.execCfg.RangeFeedFactory.New(
		fmt.Sprintf("streamID=%d", s.streamID), initialTimestamp, s.onValue, opts...,
	)

	if err := s.rf.StartFromFrontier(ctx, s.frontier); err != nil {
		s.frontier.Release()
		return err
	}

	activeStreams.Lock()
	activeStreams.m[s] = s
	activeStreams.Unlock()
	return nil
}

func (s *eventStream) setErr(err error) bool {
	if err == nil {
		return false
	}
	// we can discard an error if there is already one in the buffered channel as
	// that one will shut everything down just as well as this one.
	select {
	case s.errCh <- err:
	default:
	}
	return true
}

// Next implements eval.ValueGenerator interface.
func (s *eventStream) Next(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case err := <-s.errCh:
		return false, err
	case s.data = <-s.streamCh:
		// Re-check the err Ch
		select {
		case err := <-s.errCh:
			return false, err
		default:
			return true, nil
		}
	}
}

// Values implements eval.ValueGenerator interface.
func (s *eventStream) Values() (tree.Datums, error) {
	return s.data, nil
}

// Close implements eval.ValueGenerator interface.
func (s *eventStream) Close(ctx context.Context) {
	activeStreams.Lock()
	defer activeStreams.Unlock()
	delete(activeStreams.m, s)

	if s.rf != nil {
		s.rf.Close()
	}
	if s.frontier != nil {
		s.frontier.Release()
	}
	s.acc.Close(ctx)
}

func (s *eventStream) onInitialScanDone(ctx context.Context) {
	// We no longer expect concurrent onValue calls so we can remove the mu.
	s.addMu = nil
}

func (s *eventStream) onValues(ctx context.Context, values []kv.KeyValue) {
	// During initial-scan we expect concurrent onValue calls from the parallel
	// scan workers, but once the initial scan ends the mu will be nilled out and
	// we can avoid the locking overhead here.
	if s.addMu != nil {
		s.addMu.Lock()
		defer s.addMu.Unlock()
	}
	for _, i := range values {
		s.seb.addKV(roachpb.KeyValue{Key: i.Key, Value: *i.Value})
	}
	s.setErr(s.maybeFlushBatch(ctx))
}

func (s *eventStream) onValue(ctx context.Context, value *kvpb.RangeFeedValue) {
	// During initial-scan we expect concurrent onValue calls from the parallel
	// scan workers, but once the initial scan ends the mu will be nilled out and
	// we can avoid the locking overhead here.
	if s.addMu != nil {
		s.addMu.Lock()
		defer s.addMu.Unlock()
	}
	s.seb.addKV(roachpb.KeyValue{Key: value.Key, Value: value.Value})
	s.setErr(s.maybeFlushBatch(ctx))
}

func (s *eventStream) onCheckpoint(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
	s.debug.RF.Checkpoints.Add(1)
}

func (s *eventStream) onFrontier(ctx context.Context, timestamp hlc.Timestamp) {
	s.debug.RF.Advances.Add(1)
	s.debug.RF.LastAdvanceMicros.Store(timeutil.Now().UnixMicro())
	s.debug.RF.ResolvedMicros.Store(timestamp.GoTime().UnixMicro())
}

func (s *eventStream) onSSTable(
	ctx context.Context, sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span,
) {
	if s.setErr(s.addSST(sst, registeredSpan)) {
		return
	}
	s.setErr(s.maybeFlushBatch(ctx))
}

func (s *eventStream) onDeleteRange(ctx context.Context, delRange *kvpb.RangeFeedDeleteRange) {
	s.seb.addDelRange(*delRange)
	s.setErr(s.maybeFlushBatch(ctx))
}
func (s *eventStream) onMetadata(ctx context.Context, metadata *kvpb.RangeFeedMetadata) {
	if s.addMu != nil {
		// Split points can be sent concurrently during the initial scan.
		s.addMu.Lock()
		defer s.addMu.Unlock()
	}
	log.VInfof(ctx, 2, "received metadata event: %s, fromManualSplit: %t, parent start key %s", metadata.Span, metadata.FromManualSplit, metadata.ParentStartKey)
	if metadata.FromManualSplit && !metadata.Span.Key.Equal(metadata.ParentStartKey) {
		// Only send new manual split keys (i.e. a child rangefeed start key that
		// differs from the parent start key)
		s.seb.addSplitPoint(metadata.Span.Key)
		s.setErr(s.maybeFlushBatch(ctx))
	}
}

func (s *eventStream) maybeCheckpoint(
	ctx context.Context, advanced bool, frontier rangefeed.VisitableFrontier,
) {
	if s.addMu != nil {
		s.addMu.Lock()
		defer s.addMu.Unlock()
	}
	age := timeutil.Since(s.lastCheckpointTime)
	if (advanced && age > s.spec.Config.MinCheckpointFrequency) || (age > 2*s.spec.Config.MinCheckpointFrequency) {
		s.sendCheckpoint(ctx, frontier)
	}
}

func (s *eventStream) sendCheckpoint(ctx context.Context, frontier rangefeed.VisitableFrontier) {
	if err := s.flushBatch(ctx); err != nil {
		return
	}

	spans := make([]jobspb.ResolvedSpan, 0, s.lastCheckpointLen)
	frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		spans = append(spans, jobspb.ResolvedSpan{Span: sp, Timestamp: ts})
		return span.ContinueMatch
	})
	s.lastCheckpointLen = len(spans)

	if s.setErr(s.sendFlush(ctx, &streampb.StreamEvent{Checkpoint: &streampb.StreamEvent_StreamCheckpoint{ResolvedSpans: spans}})) {
		return
	}
	// set the local time for pacing.
	s.lastCheckpointTime = timeutil.Now()

	s.debug.Flushes.Checkpoints.Add(1)
	s.debug.LastCheckpoint.Micros.Store(s.lastCheckpointTime.UnixMicro())
	s.debug.LastCheckpoint.Spans.Store(spans)
}

func (s *eventStream) maybeFlushBatch(ctx context.Context) error {
	if s.seb.size > int(s.spec.Config.BatchByteSize) {
		return s.flushBatch(ctx)
	}
	return nil
}

func (s *eventStream) flushBatch(ctx context.Context) error {
	if s.seb.size == 0 {
		return nil
	}
	s.debug.Flushes.Batches.Add(1)
	defer s.seb.reset()
	return s.sendFlush(ctx, &streampb.StreamEvent{Batch: &s.seb.batch})
}
func (s *eventStream) sendFlush(ctx context.Context, event *streampb.StreamEvent) error {
	data, err := protoutil.Marshal(event)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.streamCh <- tree.Datums{tree.NewDBytes(tree.DBytes(data))}:
		return nil
	}
}

type checkpointPacer struct {
	pace    time.Duration
	next    time.Time
	skipped bool
}

func makeCheckpointPacer(frequency time.Duration) checkpointPacer {
	return checkpointPacer{
		pace:    frequency,
		next:    timeutil.Now().Add(frequency),
		skipped: false,
	}
}

func (p *checkpointPacer) shouldCheckpoint(
	currentFrontier hlc.Timestamp, frontierAdvanced bool,
) bool {
	now := timeutil.Now()
	enoughTimeElapsed := p.next.Before(now)

	// Handle previously skipped updates.
	// Normally, we want to emit checkpoint records when frontier advances.
	// However, checkpoints could be skipped if the frontier advanced too rapidly
	// (i.e. more rapid than MinCheckpointFrequency).  In those cases, we skip emitting
	// the checkpoint, but we will emit it at a later time.
	if p.skipped {
		if enoughTimeElapsed {
			p.skipped = false
			p.next = now.Add(p.pace)
			return true
		}
		return false
	}

	isInitialScanCheckpoint := currentFrontier.IsEmpty()
	// Handle updates when frontier advances.
	if frontierAdvanced || isInitialScanCheckpoint {
		if enoughTimeElapsed {
			p.next = now.Add(p.pace)
			return true
		}
		p.skipped = true
		return false
	}
	return false
}

// Add a RangeFeedSSTable into current batch.
func (s *eventStream) addSST(sst *kvpb.RangeFeedSSTable, registeredSpan roachpb.Span) error {
	// We send over the whole SSTable if the sst span is within
	// the registered span boundaries.
	if registeredSpan.Contains(sst.Span) {
		s.seb.addSST(*sst)
		return nil
	}
	// If the sst span exceeds boundaries of the watched spans,
	// we trim the sst data to avoid sending unnecessary data.
	// TODO(casper): add metrics to track number of SSTs, and number of ssts
	// that are not inside the boundaries (and possible count+size of kvs in such ssts).
	//
	// Extract the received SST to only contain data within the boundaries of
	// matching registered span. Execute the specified operations on each MVCC
	// key value and each MVCCRangeKey value in the trimmed SSTable.
	return replicationutils.ScanSST(sst, registeredSpan,
		func(k storage.MVCCKeyValue) error {
			// TODO(ssd): We technically get MVCCValueHeaders in our
			// SSTs. But currently there are so many ways _not_ to
			// get them that writing them here would just be
			// confusing until we fix them all.
			v, err := storage.DecodeValueFromMVCCValue(k.Value)
			if err != nil {
				return err
			}
			s.seb.addKV(roachpb.KeyValue{Key: k.Key.Key, Value: roachpb.Value{RawBytes: v.RawBytes, Timestamp: k.Key.Timestamp}})
			return nil
		}, func(rk storage.MVCCRangeKeyValue) error {
			s.seb.addDelRange(kvpb.RangeFeedDeleteRange{
				Span:      roachpb.Span{Key: rk.RangeKey.StartKey, EndKey: rk.RangeKey.EndKey},
				Timestamp: rk.RangeKey.Timestamp,
			})
			return nil
		})
}

func (s *eventStream) validateProducerJobAndSpec(ctx context.Context) (roachpb.TenantID, error) {
	producerJobID := jobspb.JobID(s.streamID)
	job, err := s.execCfg.JobRegistry.LoadJob(ctx, producerJobID)
	if err != nil {
		return roachpb.TenantID{}, err
	}
	payload := job.Payload()
	sp, ok := payload.GetDetails().(*jobspb.Payload_StreamReplication)
	if !ok {
		return roachpb.TenantID{}, notAReplicationJobError(producerJobID)
	}
	if sp.StreamReplication == nil {
		return roachpb.TenantID{}, errors.AssertionFailedf("unexpected nil StreamReplication in producer job %d payload", producerJobID)
	}
	if job.Status() != jobs.StatusRunning {
		return roachpb.TenantID{}, jobIsNotRunningError(producerJobID, job.Status(), "stream events")
	}

	// Validate that the requested spans are a subset of the
	// source tenant's keyspace.
	sourceTenantID := sp.StreamReplication.TenantID
	sourceTenantSpans := keys.MakeTenantSpan(sourceTenantID)
	for _, sp := range s.spec.Spans {
		if !sourceTenantSpans.Contains(sp) {
			err := pgerror.Newf(pgcode.InvalidParameterValue, "requested span %s is not contained within the keyspace of source tenant %d",
				sp,
				sourceTenantID)
			return roachpb.TenantID{}, err
		}
	}
	return sourceTenantID, nil
}

func (s *eventStream) DebugGetProducerStatus() *streampb.DebugProducerStatus {
	s.debug.StreamID = s.streamID
	s.debug.Spec = s.spec
	return &s.debug
}

const defaultBatchSize = 1 << 20

func setConfigDefaults(cfg *streampb.StreamPartitionSpec_ExecutionConfig) {
	const defaultInitialScanParallelism = 16
	const defaultMinCheckpointFrequency = 10 * time.Second

	if cfg.InitialScanParallelism <= 0 {
		cfg.InitialScanParallelism = defaultInitialScanParallelism
	}

	if cfg.MinCheckpointFrequency <= 0 {
		cfg.MinCheckpointFrequency = defaultMinCheckpointFrequency
	}

	if cfg.BatchByteSize <= 0 {
		cfg.BatchByteSize = defaultBatchSize
	}
}

func streamPartition(
	evalCtx *eval.Context, streamID streampb.StreamID, opaqueSpec []byte,
) (eval.ValueGenerator, error) {
	var spec streampb.StreamPartitionSpec
	if err := protoutil.Unmarshal(opaqueSpec, &spec); err != nil {
		return nil, errors.Wrapf(err, "invalid partition spec for stream %d", streamID)
	}
	if !evalCtx.SessionData().AvoidBuffering {
		return nil, errors.New("partition streaming requires 'SET avoid_buffering = true' option")
	}
	if len(spec.Spans) == 0 {
		return nil, errors.AssertionFailedf("expected at least one span, got none")
	}
	setConfigDefaults(&spec.Config)

	execCfg := evalCtx.Planner.ExecutorConfig().(*sql.ExecutorConfig)

	return &eventStream{
		streamID: streamID,
		spec:     spec,
		execCfg:  execCfg,
		mon:      evalCtx.Planner.Mon(),
	}, nil
}
