// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type showLogsNode struct {
	optColumnsSlot
	*tree.ShowLogs
	resultsCh chan tree.Datums
	row       tree.Datums
	distProc  ctxgroup.Group
	mu        struct {
		syncutil.Mutex
		swallowError bool
	}
}

var _ planNode = (*showLogsNode)(nil)

// ShowLogs returns plan node responsible for showing log files across the cluster.
func (p *planner) ShowLogs(ctx context.Context, stmt *tree.ShowLogs) (planNode, error) {
	return &showLogsNode{ShowLogs: stmt}, nil
}

// startExec implements planNode
func (n *showLogsNode) startExec(params runParams) error {
	p := params.p

	ctx := logtags.AddTag(params.ctx, "log-spy", nil)
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return err
	}
	if !hasAdmin {
		return pgerror.New(pgcode.InsufficientPrivilege, "admin privileges required for SHOW LOG")
	}

	evalCtx := p.ExtendedEvalContext()
	dsp := p.DistSQLPlanner()
	execCfg := p.ExecCfg()

	var spySpec execinfrapb.LogSpySpec
	if err := setPatternOption(ctx, p, n.Pattern, &spySpec); err != nil {
		return err
	}

	var spyDuration time.Duration
	if err := applyWithOptions(ctx, p, n.With, &spySpec, &spyDuration); err != nil {
		return err
	}

	var cancelSpy func()
	if spyDuration != 0 {
		ctx, cancelSpy = context.WithCancel(ctx)
	}

	planCtx, nodes, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, p.ExecCfg())
	if err != nil {
		return err
	}

	nodes, err = filterNodes(nodes, n.Nodes)
	if err != nil {
		return err
	}

	// Setup a one-stage plan with one proc per input spec.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(nodes))
	for i := range nodes {
		corePlacement[i].NodeID = nodes[i]
		corePlacement[i].Core.LogSpy = &spySpec
	}

	plan := planCtx.NewPhysicalPlan()

	plan.AddNoInputStage(
		corePlacement,
		execinfrapb.PostProcessSpec{},
		logSpyProcResultTypes,
		execinfrapb.Ordering{},
	)
	gatewayNodeID, err := execCfg.NodeID.OptionalNodeIDErr(48274)
	if err != nil {
		return err
	}

	plan.AddSingleGroupStage(
		gatewayNodeID,
		execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
		execinfrapb.PostProcessSpec{},
		logSpyProcResultTypes,
	)
	plan.PlanToStreamColMap = []int{0}

	dsp.FinalizePlan(planCtx, plan)

	n.resultsCh = make(chan tree.Datums)
	n.distProc = ctxgroup.WithContext(ctx)
	n.distProc.Go(func() error {
		defer close(n.resultsCh)
		var noTxn *kv.Txn
		rw := &logRowReceiver{resultsCh: n.resultsCh}
		recv := MakeDistSQLReceiver(
			ctx,
			rw,
			tree.Rows,
			nil, /* rangeCache */
			noTxn,
			nil, /* clockUpdater */
			evalCtx.Tracing,
			execCfg.ContentionRegistry,
		)
		defer recv.Release()

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		dsp.Run(planCtx, noTxn, plan, recv, &evalCtxCopy, nil /* finishedSetupFn */)()
		return rw.Err()
	})

	if cancelSpy != nil {
		n.distProc.Go(func() error {
			defer cancelSpy()

			select {
			case <-params.ctx.Done():
				// NB: use params.ctx to detect cancellation of the parent context.
				return params.ctx.Err()
			case <-time.After(spyDuration):
				n.mu.Lock()
				defer n.mu.Unlock()
				n.mu.swallowError = true
				cancelSpy()
				return nil
			}
		})
	}
	return nil
}

func setPatternOption(
	ctx context.Context, p *planner, pattern tree.Expr, spySpec *execinfrapb.LogSpySpec,
) error {
	if pattern == nil {
		return nil
	}

	optFn, err := p.TypeAsString(ctx, pattern, "show logs")
	if err != nil {
		return err
	}
	opt, err := optFn()
	if err != nil {
		return err
	}
	spySpec.Pattern = opt
	return nil
}

const (
	optVModule  = `vmodule`
	optDuration = `duration`
)

var optsExpectedValues = map[string]KVStringOptValidate{
	optVModule:  KVStringOptRequireValue,
	optDuration: KVStringOptRequireValue,
}

func applyWithOptions(
	ctx context.Context,
	p *planner,
	with tree.KVOptions,
	spySpec *execinfrapb.LogSpySpec,
	spyDuration *time.Duration,
) error {
	if len(with) == 0 {
		return nil
	}
	optsFn, err := p.TypeAsStringOpts(ctx, with, optsExpectedValues)
	if err != nil {
		return err
	}
	options, err := optsFn()
	if err != nil {
		return err
	}
	if vmodule, ok := options[optVModule]; ok {
		spySpec.VModule = vmodule
	}
	if durationStr, ok := options[optDuration]; ok {
		d, err := time.ParseDuration(durationStr)
		if err != nil {
			return err
		}
		*spyDuration = d
	}
	return nil
}

func filterNodes(nodes, includeNodes []roachpb.NodeID) ([]roachpb.NodeID, error) {
	if len(includeNodes) == 0 {
		return nodes, nil
	}

	if len(includeNodes) > len(nodes) {
		return nil, errors.Newf("cannot plan on %d nodes; only %d available", len(includeNodes), len(nodes))
	}
	filter := make(map[roachpb.NodeID]struct{})
	for _, n := range includeNodes {
		filter[n] = struct{}{}
	}
	filteredNodes := make([]roachpb.NodeID, 0, len(includeNodes))
	for _, n := range nodes {
		if _, ok := filter[n]; ok {
			filteredNodes = append(filteredNodes, n)
		}
	}
	if len(filteredNodes) == 0 {
		return nil, errors.Newf("could not plan on nodes %v; 0 available after filtering %v", includeNodes, nodes)
	}
	return filteredNodes, nil
}

// Next implements planNode
func (n *showLogsNode) Next(params runParams) (bool, error) {
	maybeReturnError := func(err error) (bool, error) {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.mu.swallowError {
			return false, nil
		}
		return false, err
	}

	select {
	case <-params.ctx.Done():
		return maybeReturnError(params.ctx.Err())
	case row, ok := <-n.resultsCh:
		if ok {
			n.row = row
			return true, nil
		}
		// Dist flow exited.
		return maybeReturnError(params.ctx.Err())
	}
}

// Values implements planNode
func (n *showLogsNode) Values() tree.Datums {
	return n.row
}

// Close implements planNode
func (n *showLogsNode) Close(ctx context.Context) {
	_ = ctx
}

const maxLogMessagesToBuffer = 1024

// logBuf contains a limited buffer of log messages.
// receiver methods on this object must *never* call logging
// methods -- doing so will deadlock.
type logBuf struct {
	syncutil.Mutex
	filter *regexp.Regexp
	buf    [][]byte
	pos    int
	full   bool
}

// Intercept implements log.LogMessageInterceptor
func (b *logBuf) Intercept(message []byte) {
	if b.filter != nil && !b.filter.Match(message) {
		return
	}

	b.Lock()
	defer b.Unlock()

	if b.buf == nil {
		b.buf = make([][]byte, maxLogMessagesToBuffer)
	}

	// Copy message
	if cap(b.buf[b.pos]) < len(message) {
		b.buf[b.pos] = make([]byte, len(message))
	} else {
		b.buf[b.pos] = b.buf[b.pos][:len(message)]
	}
	copy(b.buf[b.pos], message)
	b.pos = (b.pos + 1) % cap(b.buf)
	if b.pos == 0 {
		b.full = true
	}
}

const logBatchSize = 32

func (b *logBuf) len() int {
	b.Lock()
	defer b.Unlock()
	if b.full {
		return cap(b.buf)
	}
	return b.pos
}

// drain returns accumulated log messages, and reset this buffer.
func (b *logBuf) drain(sink *execinfrapb.LogEntryBatch) {
	b.Lock()
	defer b.Unlock()

	numMessages := b.pos
	start := 0
	if b.full {
		sink.Truncated = true
		numMessages = cap(b.buf)
		start = b.pos
	}

	for i := start; i < start+numMessages; i++ {
		pos := i % maxLogMessagesToBuffer
		messageCopy := make([]byte, len(b.buf[pos]))
		copy(messageCopy, b.buf[pos])
		sink.Entries = append(sink.Entries, messageCopy)
		b.buf[pos] = b.buf[pos][:0]
	}

	b.pos = 0
	b.full = false
}

type spyProc struct {
	execinfra.ProcessorBase
	execinfra.StreamingProcessor

	ctx           context.Context
	spec          execinfrapb.LogSpySpec
	resetVModule  *string
	nodeID        roachpb.NodeID
	interceptorID log.InterceptorID
	buf           logBuf             // log message buffer
	row           [1]rowenc.EncDatum // Row to emit in Next()
}

var _ execinfra.Processor = (*spyProc)(nil)
var _ execinfra.RowSource = (*spyProc)(nil)

var logSpyProcResultTypes = []*types.T{types.Bytes}

// Start implements RowSource.
func (s *spyProc) Start(ctx context.Context) {
	s.ctx = s.StartInternal(ctx, `logspy`)
	nodeID, err := s.FlowCtx.NodeID.OptionalNodeIDErr(48274)
	if err != nil {
		s.MoveToDraining(err)
		return
	}
	s.nodeID = nodeID

	if s.spec.Pattern != "" {
		filter, err := regexp.Compile(s.spec.Pattern)
		if err != nil {
			s.MoveToDraining(err)
			return
		}
		s.buf.filter = filter
	}

	if s.spec.VModule != "" {
		oldVModule, err := log.SetVModuleWithOldValue(s.spec.VModule)
		if err != nil {
			s.MoveToDraining(err)
			return
		}
		if oldVModule != s.spec.VModule {
			s.resetVModule = &oldVModule
		}
	}

	s.interceptorID = log.AddInterceptor(&s.buf)
}

// Next implements RowSource
func (s *spyProc) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	const drainFrequency = time.Second
	var ticker *time.Ticker
	for l := s.buf.len(); l == 0 || (l < logBatchSize && ticker == nil); l = s.buf.len() {
		if ticker == nil {
			ticker = time.NewTicker(drainFrequency)
			defer ticker.Stop()
		}

		select {
		case <-s.ctx.Done():
			if s.State == execinfra.StateRunning {
				s.restoreLoggingSettings()
				s.MoveToDraining(s.ctx.Err())
			}

			return nil, nil
		case <-ticker.C:
		}
	}

	var batch execinfrapb.LogEntryBatch
	batch.NodeID = s.nodeID
	s.buf.drain(&batch)

	data, err := protoutil.Marshal(&batch)
	if err != nil {
		return nil, &execinfrapb.ProducerMetadata{Err: err}
	}
	s.row[0] = rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(data)))
	return s.row[:], nil
}

func (s *spyProc) restoreLoggingSettings() {
	log.RemoveInterceptor(s.interceptorID)
	if s.resetVModule != nil {
		_ = log.SetVModule(*s.resetVModule)
	}
}

// ConsumerClosed implements RowSource.
func (s *spyProc) ConsumerClosed() {
}

func newLogSpyProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.LogSpySpec,
	post *execinfrapb.PostProcessSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	sp := &spyProc{
		spec: spec,
	}
	if err := sp.Init(
		sp,
		post,
		logSpyProcResultTypes,
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	return sp, nil
}

type logRowReceiver struct {
	resultsCh chan<- tree.Datums
	err       error
}

var _ rowResultWriter = (*logRowReceiver)(nil)

func writeMessage(ctx context.Context, msg []byte, ch chan<- tree.Datums) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- tree.Datums{tree.NewDString(string(msg))}:
		return nil
	}
}

// AddRow implements rowResultWriter
func (r *logRowReceiver) AddRow(ctx context.Context, row tree.Datums) error {
	var batch execinfrapb.LogEntryBatch
	if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(row[0])), &batch); err != nil {
		return err
	}

	if batch.Truncated {
		msg := []byte(fmt.Sprintf("[n%d] ... truncated ...", batch.NodeID))
		if err := writeMessage(ctx, msg, r.resultsCh); err != nil {
			return err
		}

	}

	for _, entry := range batch.Entries {
		entry = bytes.TrimSuffix(entry, []byte{'\n'})
		if err := writeMessage(ctx, entry, r.resultsCh); err != nil {
			return err
		}
	}
	return nil
}

// IncrementRowsAffected implements rowResultWriter.
func (r *logRowReceiver) IncrementRowsAffected(_ int) {
}

// SetError implements rowResultWriter.
func (r *logRowReceiver) SetError(err error) {
	r.err = err
}

// Err implements rowResultWriter.
func (r *logRowReceiver) Err() error {
	return r.err
}

func init() {
	rowexec.NewLogSpyProcessor = newLogSpyProcessor
}
