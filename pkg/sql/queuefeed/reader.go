package queuefeed

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type readerState int

const (
	readerStateIdle readerState = iota
	readerStateHasUncommittedBatch
)

// has rangefeed on data. reads from it. handles handoff
// state machine around handing out batches and handing stuff off
type Reader struct {
	executor isql.DB
	rff      *rangefeed.Factory
	mgr      *Manager
	name     string

	mu struct {
		syncutil.Mutex
		state          readerState
		buf            []tree.Datums
		inflightBuffer []tree.Datums
	}

	cancel context.CancelFunc
}

func NewReader(ctx context.Context, executor isql.DB, mgr *Manager, name string) *Reader {
	buf := []tree.Datums{
		{tree.NewDString("1"), tree.NewDString("2"), tree.NewDString("3")},
		{tree.NewDString("4"), tree.NewDString("5"), tree.NewDString("6")},
		{tree.NewDString("7"), tree.NewDString("8"), tree.NewDString("9")},
	}

	r := &Reader{
		executor: executor,
		mgr:      mgr,
		name:     name,
	}
	r.mu.state = readerStateIdle
	r.mu.buf = buf
	r.mu.inflightBuffer = make([]tree.Datums, 0)

	ctx, cancel := context.WithCancel(ctx)
	r.cancel = cancel

	// TODO(queuefeed): Re-enable once queue data table and spans are implemented
	// r.setupRangefeed(ctx)
	go r.run(ctx)
	return r
}

func (r *Reader) setupRangefeed(ctx context.Context) {
	incomingResolveds := make(chan hlc.Timestamp)
	setErr := func(err error) { r.cancel() }

	onValue := func(ctx context.Context, value *kvpb.RangeFeedValue) {
		r.mu.Lock()
		defer r.mu.Unlock()

		if len(r.mu.buf) > 100 {
			// TODO: wait for rows to be read before adding more
		}
		// TODO: decode value.Value
		r.mu.buf = append(r.mu.buf, tree.Datums{tree.DVoidDatum})
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
	defer rf.Close()

}

// - [x] setup rangefeed on data
// - [ ] handle only watching my partitions
// - [ ] after each batch, ask mgr if i need to change assignments
// - [ ] buffer rows in the background before being asked for them
// - [ ] checkpoint frontier if our frontier has advanced and we confirmed receipt
// - [ ] gonna need some way to clean stuff up on conn_executor.close()

func (r *Reader) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

    // state machine
	// -

	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func (r *Reader) GetRows(ctx context.Context, limit int) ([]tree.Datums, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.mu.state != readerStateIdle {
		return nil, errors.New("reader not idle")
	}
	if len(r.mu.inflightBuffer) > 0 {
		return nil, errors.AssertionFailedf("getrows called with nonempty inflight buffer")
	}

	if limit > len(r.mu.buf) {
		limit = len(r.mu.buf)
	}

	r.mu.inflightBuffer = append(r.mu.inflightBuffer, r.mu.buf[0:limit]...)
	r.mu.buf = r.mu.buf[limit:]

	r.mu.inflightBuffer = append(r.mu.inflightBuffer, r.mu.buf...)
	clear(r.mu.buf)
	r.mu.state = readerStateHasUncommittedBatch
	return r.mu.inflightBuffer, nil

	// and then trigger the goro to check if m wants us to change assignments
	// if it does, handle that stuff before doing a new batch
}

func (r *Reader) ConfirmReceipt(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.mu.state = readerStateIdle
	clear(r.mu.inflightBuffer)

	return nil
}
