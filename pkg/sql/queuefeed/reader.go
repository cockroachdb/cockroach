package queuefeed

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/queuefeed/queuebase"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const maxBufSize = 1000

type readerState int

const (
	readerStateBatching readerState = iota
	readerStateHasUncommittedBatch
	readerStateCheckingForReassignment
	readerStateDead
)

// bufferedEvent represents either a data row or a checkpoint timestamp
// in the reader's buffer. Exactly one of row or resolved will be set.
type bufferedEvent struct {
	// row is set for data events. nil for checkpoint events.
	row tree.Datums
	// resolved is set for checkpoint events. Empty for data events.
	resolved hlc.Timestamp
}

// has rangefeed on data. reads from it. handles handoff
// state machine around handing out batches and handing stuff off
type Reader struct {
	executor isql.DB
	rff      *rangefeed.Factory
	mgr      *Manager
	name     string
	assigner *PartitionAssignments

	// stuff for decoding data. this is ripped from rowfetcher_cache.go in changefeeds
	codec    keys.SQLCodec
	leaseMgr *lease.Manager

	mu struct {
		syncutil.Mutex
		state          readerState
		buf            []bufferedEvent
		inflightBuffer []bufferedEvent
		poppedWakeup   *sync.Cond
		pushedWakeup   *sync.Cond
	}

	// TODO: handle the case where an assignment can change.
	session    Session
	assignment *Assignment
	rangefeed  *rangefeed.RangeFeed

	cancel     context.CancelCauseFunc
	goroCtx    context.Context
	isShutdown atomic.Bool
}

func NewReader(
	ctx context.Context,
	executor isql.DB,
	mgr *Manager,
	rff *rangefeed.Factory,
	codec keys.SQLCodec,
	leaseMgr *lease.Manager,
	session Session,
	assigner *PartitionAssignments,
	name string,
) (*Reader, error) {
	r := &Reader{
		executor: executor,
		mgr:      mgr,
		codec:    codec,
		leaseMgr: leaseMgr,
		name:     name,
		rff:      rff,
		// stored so we can use it in methods using a different context than the main goro ie GetRows and ConfirmReceipt
		goroCtx:  ctx,
		assigner: assigner,
		session:  session,
	}
	r.mu.state = readerStateBatching
	r.mu.buf = make([]bufferedEvent, 0, maxBufSize)
	r.mu.poppedWakeup = sync.NewCond(&r.mu.Mutex)
	r.mu.pushedWakeup = sync.NewCond(&r.mu.Mutex)

	ctx, cancel := context.WithCancelCause(ctx)
	r.cancel = func(cause error) {
		fmt.Printf("canceling with cause: %s\n", cause)
		cancel(cause)
		r.mu.poppedWakeup.Broadcast()
		r.mu.pushedWakeup.Broadcast()
	}

	assignment, err := assigner.RegisterSession(ctx, session)
	if err != nil {
		return nil, errors.Wrap(err, "registering session for reader")
	}
	if err := r.setupRangefeed(ctx, assignment); err != nil {
		return nil, errors.Wrap(err, "setting up rangefeed")
	}
	go r.run(ctx)
	return r, nil
}

var ErrNoPartitionsAssigned = errors.New("no partitions assigned to reader: todo support this case by polling for assignment")

func (r *Reader) setupRangefeed(ctx context.Context, assignment *Assignment) error {
	defer func() {
		fmt.Println("setupRangefeed done")
	}()

	// TODO: handle the case where there are no partitions in the assignment. In
	// that case we should poll `RefreshAssignment` until we get one. This would
	// only occur if every assignment was handed out already.
	if len(assignment.Partitions) == 0 {
		return errors.Wrap(ErrNoPartitionsAssigned, "setting up rangefeed")
	}

	onValue := func(ctx context.Context, value *kvpb.RangeFeedValue) {
		fmt.Printf("onValue: %+v\n", value)
		r.mu.Lock()
		defer r.mu.Unlock()

		// wait for rows to be read before adding more, if necessary
		for ctx.Err() == nil && len(r.mu.buf) > maxBufSize {
			r.mu.poppedWakeup.Wait()
		}

		if !value.Value.IsPresent() {
			// not handling diffs/deletes rn
			return
		}
		datums, err := r.decodeRangefeedValue(ctx, value)
		if err != nil {
			r.cancel(errors.Wrapf(err, "decoding rangefeed value: %+v", value))
			return
		}
		r.mu.buf = append(r.mu.buf, bufferedEvent{row: datums})
		r.mu.pushedWakeup.Broadcast()
		fmt.Printf("onValue done with buf len: %d\n", len(r.mu.buf))
	}
	// setup rangefeed on data
	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("queuefeed.reader", fmt.Sprintf("name=%s", r.name)),
		// rangefeed.WithMemoryMonitor(w.mon),
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
			// This can happen when done catching up; ignore it.
			if checkpoint.ResolvedTS.IsEmpty() {
				return
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			// Wait for rows to be read before adding more, if necessary.
			for ctx.Err() == nil && len(r.mu.buf) > maxBufSize {
				r.mu.poppedWakeup.Wait()
			}

			if ctx.Err() != nil {
				return
			}

			r.mu.buf = append(r.mu.buf, bufferedEvent{resolved: checkpoint.ResolvedTS})
		}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) { r.cancel(err) }),
		rangefeed.WithConsumerID(42),
		rangefeed.WithInvoker(func(fn func() error) error { return fn() }),
		rangefeed.WithFiltering(false),
	}

	// Resume from checkpoint if available
	// TODO: Support multiple partitions
	partitionID := int64(1)
	initialTS, err := r.mgr.ReadCheckpoint(ctx, r.name, partitionID)
	if err != nil {
		return errors.Wrap(err, "reading checkpoint")
	}
	if initialTS.IsEmpty() {
		// No checkpoint found, start from now
		initialTS = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	}

	rf := r.rff.New(
		fmt.Sprintf("queuefeed.reader.name=%s", r.name), initialTS, onValue, opts...,
	)

	spans := assignment.Spans()

	fmt.Printf("starting rangefeed with spans: %+v\n", spans)

	if err := rf.Start(ctx, spans); err != nil {
		return errors.Wrap(err, "starting rangefeed")
	}

	r.rangefeed = rf
	r.assignment = assignment
	return nil
}

// - [x] setup rangefeed on data
// - [X] handle only watching my partitions
// - [X] after each batch, ask mgr if i need to  assignments
// - [X] buffer rows in the background before being asked for them
// - [ ] checkpoint frontier if our frontier has advanced and we confirmed receipt
// - [X] gonna need some way to clean stuff up on conn_executor.close()

// TODO: this loop isnt doing much anymore. if we dont need it for anything else, let's remove it
func (r *Reader) run(ctx context.Context) {
	defer func() {
		fmt.Println("run done")
		r.isShutdown.Store(true)
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("run: ctx done: %s; cause: %s\n", ctx.Err(), context.Cause(ctx))
			return
		}
	}
}

func (r *Reader) GetRows(ctx context.Context, limit int) ([]tree.Datums, error) {
	fmt.Printf("GetRows start\n")

	if r.isShutdown.Load() {
		return nil, errors.New("reader is shutting down")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.state != readerStateBatching {
		return nil, errors.New("reader not idle")
	}
	if len(r.mu.inflightBuffer) > 0 {
		return nil, errors.AssertionFailedf("getrows called with nonempty inflight buffer")
	}

	// Helper to count data events (not checkpoints) in buffer
	hasDataEvents := func() bool {
		for _, event := range r.mu.buf {
			if event.resolved.IsEmpty() {
				return true
			}
		}
		return false
	}

	// Wait until we have at least one data event (not just checkpoints)
	if !hasDataEvents() {
		// shut down the reader if this ctx (which is distinct from the goro ctx) is canceled
		defer context.AfterFunc(ctx, func() {
			r.cancel(errors.Wrapf(ctx.Err(), "GetRows canceled"))
		})()
		for ctx.Err() == nil && r.goroCtx.Err() == nil && !hasDataEvents() {
			r.mu.pushedWakeup.Wait()
		}
		if ctx.Err() != nil {
			return nil, errors.Wrapf(ctx.Err(), "GetRows canceled")
		}
	}

	// Find the position of the (limit+1)th data event (not checkpoint)
	// We'll take everything up to that point, which gives us up to `limit` data rows
	// plus any checkpoints that came before/between them.
	bufferEndIdx := len(r.mu.buf)

	// Optimization: if the entire buffer is smaller than limit, take it all
	if len(r.mu.buf) > limit {
		dataCount := 0
		for i, event := range r.mu.buf {
			if event.resolved.IsEmpty() {
				dataCount++
				if dataCount > limit {
					bufferEndIdx = i
					break
				}
			}
		}
	}

	r.mu.inflightBuffer = append(r.mu.inflightBuffer, r.mu.buf[0:bufferEndIdx]...)
	r.mu.buf = r.mu.buf[bufferEndIdx:]

	r.mu.state = readerStateHasUncommittedBatch
	r.mu.poppedWakeup.Broadcast()

	// Here we filter to return only data events to the user.
	result := make([]tree.Datums, 0, limit)
	for _, event := range r.mu.inflightBuffer {
		if event.resolved.IsEmpty() {
			result = append(result, event.row)
		}
	}

	return result, nil
}

// ConfirmReceipt is called when we commit a transaction that reads from the queue.
// We will checkpoint if we have checkpoint events in our inflightBuffer.
func (r *Reader) ConfirmReceipt(ctx context.Context) {
	if r.isShutdown.Load() {
		return
	}

	var checkpointToWrite hlc.Timestamp
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		// Find the last checkpoint in inflightBuffer
		for _, event := range r.mu.inflightBuffer {
			if !event.resolved.IsEmpty() {
				checkpointToWrite = event.resolved
			}
		}

		r.mu.inflightBuffer = r.mu.inflightBuffer[:0]
		r.mu.state = readerStateCheckingForReassignment
	}()

	// Persist the checkpoint if we have one.
	if !checkpointToWrite.IsEmpty() {
		// TODO: Support multiple partitions - for now we only have partition 1.
		partitionID := int64(1)
		if err := r.mgr.WriteCheckpoint(ctx, r.name, partitionID, checkpointToWrite); err != nil {
			fmt.Printf("error writing checkpoint: %s\n", err)
			// TODO: decide how to handle checkpoint write errors. Since the txn
			// has already committed, I don't think we can really fail at this point.
		}
	}

	select {
	case <-ctx.Done():
		return
	case <-r.goroCtx.Done():
		return
	default:
		// TODO only set caughtUp to true if our frontier is near the current time.
		newAssignment, err := r.assigner.RefreshAssignment(ctx, r.session /*caughtUp=*/, true)
		if err != nil {
			r.cancel(errors.Wrap(err, "refreshing assignment"))
			return
		}
		if newAssignment != nil {
			if err := r.updateAssignment(newAssignment); err != nil {
				r.cancel(errors.Wrap(err, "updating assignment"))
				return
			}
		}
	}
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.mu.state = readerStateBatching
	}()
}

func (r *Reader) RollbackBatch(ctx context.Context) {
	if r.isShutdown.Load() {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	newBuf := make([]bufferedEvent, 0, len(r.mu.inflightBuffer)+len(r.mu.buf))
	newBuf = append(newBuf, r.mu.inflightBuffer...)
	newBuf = append(newBuf, r.mu.buf...)
	r.mu.buf = newBuf
	r.mu.inflightBuffer = r.mu.inflightBuffer[:0]

	r.mu.state = readerStateBatching
}

func (r *Reader) IsAlive() bool {
	return !r.isShutdown.Load()
}

func (r *Reader) Close() error {
	err := r.assigner.UnregisterSession(r.goroCtx, r.session)
	r.cancel(errors.New("reader closing"))
	r.rangefeed.Close()
	return err
}

func (r *Reader) updateAssignment(assignment *Assignment) error {
	defer func() {
		fmt.Printf("updateAssignment done with assignment: %+v\n", assignment)
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	r.assignment = assignment
	r.rangefeed.Close()
	r.mu.buf = r.mu.buf[:0]

	if err := r.setupRangefeed(r.goroCtx, assignment); err != nil {
		return errors.Wrapf(err, "setting up rangefeed for new assignment: %+v", assignment)
	}
	return nil
}

func (r *Reader) checkForReassignment(ctx context.Context) error {
	defer func() {
		fmt.Println("checkForReassignment done")
	}()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.state != readerStateCheckingForReassignment {
		return errors.AssertionFailedf("reader not in checking for reassignment state")
	}

	change, err := r.mgr.reassessAssignments(ctx, r.name)
	if err != nil {
		return errors.Wrap(err, "reassessing assignments")
	}
	if change {
		fmt.Println("TODO: reassignment detected. lets do something about it")
	}
	r.mu.state = readerStateBatching
	return nil
}

func (r *Reader) decodeRangefeedValue(
	ctx context.Context, rfv *kvpb.RangeFeedValue,
) (tree.Datums, error) {
	key, value := rfv.Key, rfv.Value
	key, err := r.codec.StripTenantPrefix(key)
	if err != nil {
		return nil, errors.Wrapf(err, "stripping tenant prefix: %s", keys.PrettyPrint(nil, key))
	}

	_, tableID, _, err := rowenc.DecodePartialTableIDIndexID(key)
	if err != nil {
		return nil, errors.Wrapf(err, "decoding partial table id index id: %s", keys.PrettyPrint(nil, key))
	}
	tableDesc, err := r.fetchTableDesc(ctx, tableID, value.Timestamp)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching table descriptor: %s", keys.PrettyPrint(nil, key))
	}
	familyDesc, err := catalog.MustFindFamilyByID(tableDesc, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching family descriptor: %s", keys.PrettyPrint(nil, key))
	}
	cols, err := getRelevantColumnsForFamily(tableDesc, familyDesc)
	if err != nil {
		return nil, errors.Wrapf(err, "getting relevant columns for family: %s", keys.PrettyPrint(nil, key))
	}

	var spec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(&spec, r.codec, tableDesc, tableDesc.GetPrimaryIndex(), cols); err != nil {
		return nil, errors.Wrapf(err, "initializing index fetch spec: %s", keys.PrettyPrint(nil, key))
	}
	rf := row.Fetcher{}
	if err := rf.Init(ctx, row.FetcherInitArgs{
		Spec:              &spec,
		WillUseKVProvider: true,
		TraceKV:           true,
		TraceKVEvery:      &util.EveryN{N: 1},
	}); err != nil {
		return nil, errors.Wrapf(err, "initializing row fetcher: %s", keys.PrettyPrint(nil, key))
	}
	kvProvider := row.KVProvider{KVs: []roachpb.KeyValue{{Key: key, Value: value}}}
	if err := rf.ConsumeKVProvider(ctx, &kvProvider); err != nil {
		return nil, errors.Wrapf(err, "consuming kv provider: %s", keys.PrettyPrint(nil, key))
	}
	encDatums, _, err := rf.NextRow(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "fetching next row: %s", keys.PrettyPrint(nil, key))
	}
	_ = encDatums

	datums := make(tree.Datums, len(cols))
	for i, colID := range cols {
		col, err := catalog.MustFindColumnByID(tableDesc, colID)
		if err != nil {
			return nil, errors.Wrapf(err, "finding column by id: %d", colID)
		}
		ed := encDatums[i]
		if err := ed.EnsureDecoded(col.ColumnDesc().Type, &tree.DatumAlloc{}); err != nil {
			return nil, errors.Wrapf(err, "error decoding column %q as type %s", col.ColumnDesc().Name, col.ColumnDesc().Type.String())
		}
		datums[i] = ed.Datum
	}
	return datums, nil
}

func (r *Reader) fetchTableDesc(
	ctx context.Context, tableID descpb.ID, ts hlc.Timestamp,
) (catalog.TableDescriptor, error) {
	// Retrieve the target TableDescriptor from the lease manager. No caching
	// is attempted because the lease manager does its own caching.
	desc, err := r.leaseMgr.Acquire(ctx, lease.TimestampToReadTimestamp(ts), tableID)
	if err != nil {
		// Manager can return all kinds of errors during chaos, but based on
		// its usage, none of them should ever be terminal.
		return nil, changefeedbase.MarkRetryableError(err)
	}
	tableDesc := desc.Underlying().(catalog.TableDescriptor)
	// Immediately release the lease, since we only need it for the exact
	// timestamp requested.
	desc.Release(ctx)
	if tableDesc.MaybeRequiresTypeHydration() {
		return nil, errors.AssertionFailedf("type hydration not supported yet")
	}
	return tableDesc, nil
}

var _ queuebase.Reader = &Reader{}

func getRelevantColumnsForFamily(
	tableDesc catalog.TableDescriptor, familyDesc *descpb.ColumnFamilyDescriptor,
) ([]descpb.ColumnID, error) {
	cols := tableDesc.GetPrimaryIndex().CollectKeyColumnIDs()
	for _, colID := range familyDesc.ColumnIDs {
		cols.Add(colID)
	}

	// Maintain the ordering of tableDesc.PublicColumns(), which is
	// matches the order of columns in the SQL table.
	idx := 0
	result := make([]descpb.ColumnID, cols.Len())
	visibleColumns := tableDesc.PublicColumns()
	if tableDesc.GetDeclarativeSchemaChangerState() != nil {
		hasMergedIndex := catalog.HasDeclarativeMergedPrimaryIndex(tableDesc)
		visibleColumns = make([]catalog.Column, 0, cols.Len())
		for _, col := range tableDesc.AllColumns() {
			if col.Adding() {
				continue
			}
			if tableDesc.GetDeclarativeSchemaChangerState() == nil && !col.Public() {
				continue
			}
			if col.Dropped() && (!col.WriteAndDeleteOnly() || hasMergedIndex) {
				continue
			}
			visibleColumns = append(visibleColumns, col)
		}
		// Recover the order of the original columns.
		slices.SortStableFunc(visibleColumns, func(a, b catalog.Column) int {
			return int(a.GetPGAttributeNum()) - int(b.GetPGAttributeNum())
		})
	}
	for _, col := range visibleColumns {
		colID := col.GetID()
		if cols.Contains(colID) {
			result[idx] = colID
			idx++
		}
	}

	// Some columns in familyDesc.ColumnIDs may not be public, so
	// result may contain fewer columns than cols.
	result = result[:idx]
	return result, nil
}
