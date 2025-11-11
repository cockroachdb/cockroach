package queuefeed

import (
	"context"
	"fmt"
	"slices"
	"sync"

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
)

// has rangefeed on data. reads from it. handles handoff
// state machine around handing out batches and handing stuff off
type Reader struct {
	executor isql.DB
	rff      *rangefeed.Factory
	mgr      *Manager
	name     string
	tableID  descpb.ID

	// stuff for decoding data. this is ripped from rowfetcher_cache.go in changefeeds
	codec    keys.SQLCodec
	leaseMgr *lease.Manager

	mu struct {
		syncutil.Mutex
		state          readerState
		buf            []tree.Datums
		inflightBuffer []tree.Datums
		poppedWakeup   *sync.Cond
		pushedWakeup   *sync.Cond
	}

	triggerCheckForReassignment chan struct{}

	cancel  context.CancelCauseFunc
	goroCtx context.Context
}

func NewReader(ctx context.Context, executor isql.DB, mgr *Manager, rff *rangefeed.Factory, codec keys.SQLCodec, leaseMgr *lease.Manager, name string, tableDescID int64) *Reader {
	r := &Reader{
		executor:                    executor,
		mgr:                         mgr,
		codec:                       codec,
		leaseMgr:                    leaseMgr,
		name:                        name,
		rff:                         rff,
		tableID:                     descpb.ID(tableDescID),
		triggerCheckForReassignment: make(chan struct{}),
		// stored so we can use it in methods using a different context than the main goro ie GetRows and ConfirmReceipt
		goroCtx: ctx,
	}
	r.mu.state = readerStateBatching
	r.mu.buf = make([]tree.Datums, 0, maxBufSize)
	r.mu.poppedWakeup = sync.NewCond(&r.mu.Mutex)
	r.mu.pushedWakeup = sync.NewCond(&r.mu.Mutex)

	ctx, cancel := context.WithCancelCause(ctx)
	r.cancel = func(cause error) {
		fmt.Printf("canceling with cause: %s\n", cause)
		cancel(cause)
		r.mu.poppedWakeup.Broadcast()
		r.mu.pushedWakeup.Broadcast()
	}

	r.setupRangefeed(ctx)
	go r.run(ctx)
	return r
}

func (r *Reader) setupRangefeed(ctx context.Context) {
	defer func() {
		fmt.Println("setupRangefeed done")
	}()

	incomingResolveds := make(chan hlc.Timestamp)
	setErr := func(err error) { r.cancel(err) }

	onValue := func(ctx context.Context, value *kvpb.RangeFeedValue) {
		fmt.Printf("onValue: %+v\n", value)
		r.mu.Lock()
		defer r.mu.Unlock()

		// wait for rows to be read before adding more, if necessary
		for ctx.Err() == nil && len(r.mu.buf) > maxBufSize {
			r.mu.poppedWakeup.Wait()
		}

		datums, err := r.decodeRangefeedValue(ctx, value)
		if err != nil {
			setErr(errors.Wrapf(err, "decoding rangefeed value: %+v", value))
			return
		}
		r.mu.buf = append(r.mu.buf, datums)
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
			select {
			case incomingResolveds <- checkpoint.ResolvedTS:
			case <-ctx.Done():
			default: // TODO: handle resolveds (dont actually default here)
			}
		}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) { setErr(err) }),
		rangefeed.WithConsumerID(42),
		rangefeed.WithInvoker(func(fn func() error) error { return fn() }),
		rangefeed.WithFiltering(false),
	}

	// TODO: resume from cursor
	initialTS := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	rf := r.rff.New(
		fmt.Sprintf("queuefeed.reader.name=%s", r.name), initialTS, onValue, opts...,
	)

	// get desc for table
	desc, err := r.leaseMgr.AcquireByName(ctx, lease.TimestampToReadTimestamp(initialTS), 100, 101, "t")
	if err != nil {
		setErr(err)
		return
	}
	defer desc.Release(ctx)

	// TODO: why are we given a zero codec?
	r.codec = keys.MakeSQLCodec(roachpb.SystemTenantID)

	tk := roachpb.Span{
		Key: r.codec.TablePrefix(uint32(r.tableID)),
	}
	tk.EndKey = tk.Key.PrefixEnd()
	spans := []roachpb.Span{tk}

	fmt.Printf("starting rangefeed with spans: %+v\n", spans)

	if err := rf.Start(ctx, spans); err != nil {
		setErr(err)
		return
	}
	_ = rf
	// TODO:  rf.Close() on close

}

// - [x] setup rangefeed on data
// - [ ] handle only watching my partitions
// - [ ] after each batch, ask mgr if i need to change assignments
// - [ ] buffer rows in the background before being asked for them
// - [ ] checkpoint frontier if our frontier has advanced and we confirmed receipt
// - [ ] gonna need some way to clean stuff up on conn_executor.close()

// TODO: run still shuts down with context canceled after getting rows. why?

func (r *Reader) run(ctx context.Context) {
	defer func() {
		fmt.Println("run done")
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("run: ctx done: %s; cause: %s\n", ctx.Err(), context.Cause(ctx))
			return
		case <-r.triggerCheckForReassignment:
			fmt.Printf("triggerCheckForReassignment\n")
			if err := r.checkForReassignment(ctx); err != nil {
				r.cancel(err)
				return
			}
		}
	}
}

func (r *Reader) GetRows(ctx context.Context, limit int) ([]tree.Datums, error) {
	fmt.Printf("GetRows start\n")

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.state != readerStateBatching {
		return nil, errors.New("reader not idle")
	}
	if len(r.mu.inflightBuffer) > 0 {
		return nil, errors.AssertionFailedf("getrows called with nonempty inflight buffer")
	}

	if len(r.mu.buf) == 0 {
		fmt.Printf("GetRows called with empty buf. waiting for pushedWakeup\n")
		// shut down the reader if this ctx (which is distinct from the goro ctx) is canceled
		done := make(chan struct{})
		defer close(done)
		go func() {
			select {
			case <-ctx.Done():
				r.cancel(errors.Wrapf(context.Cause(ctx), "GetRows canceled"))
			case <-done:
				return
			}
		}()
		for ctx.Err() == nil && r.goroCtx.Err() == nil && len(r.mu.buf) == 0 {
			r.mu.pushedWakeup.Wait()
		}
		if ctx.Err() != nil {
			return nil, errors.Wrapf(context.Cause(ctx), "GetRows canceled")
		}
		if r.goroCtx.Err() != nil {
			return nil, errors.Wrapf(context.Cause(r.goroCtx), "reader shutting down")
		}
	}

	if limit > len(r.mu.buf) {
		limit = len(r.mu.buf)
	}

	fmt.Printf("GetRows called with limit: %d, buf len: %d\n", limit, len(r.mu.buf))

	r.mu.inflightBuffer = append(r.mu.inflightBuffer, r.mu.buf[0:limit]...)
	r.mu.buf = r.mu.buf[limit:]

	r.mu.state = readerStateHasUncommittedBatch

	r.mu.poppedWakeup.Broadcast()

	fmt.Printf("GetRows done with inflightBuffer len: %d, buf len: %d\n", len(r.mu.inflightBuffer), len(r.mu.buf))

	return slices.Clone(r.mu.inflightBuffer), nil

	// and then trigger the goro to check if m wants us to change assignments
	// if it does, handle that stuff before doing a new batch
}

func (r *Reader) ConfirmReceipt(ctx context.Context) {
	func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		fmt.Printf("confirming receipt with inflightBuffer len: %d\n", len(r.mu.inflightBuffer))

		clear(r.mu.inflightBuffer)
		r.mu.state = readerStateCheckingForReassignment
	}()

	select {
	case <-ctx.Done():
		return
	case <-r.goroCtx.Done():
		return
	case r.triggerCheckForReassignment <- struct{}{}:
	}
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

// TODO: this is all highly sus
func (r *Reader) decodeRangefeedValue(ctx context.Context, rfv *kvpb.RangeFeedValue) (tree.Datums, error) {
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
			return nil, errors.Wrapf(err, "finding column by id: %s", colID)
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
