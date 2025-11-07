package queuefeed

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	mgr      *Manager
	name     string

	state readerState

	buf            []tree.Datums
	inflightBuffer []tree.Datums
}

func NewReader(ctx context.Context, executor isql.DB, mgr *Manager, name string) *Reader {
	buf := []tree.Datums{
		{tree.NewDString("1"), tree.NewDString("2"), tree.NewDString("3")},
	}

	r := &Reader{
		executor:       executor,
		mgr:            mgr,
		name:           name,
		buf:            buf,
		inflightBuffer: make([]tree.Datums, 0),
	}
	go r.run(ctx)
	return r
}

// setup rangefeed on data
// handle only watching my partitions
// after each batch, ask mgr if i need to change assignments
// buffer rows in the background before being asked for them
// checkpoint frontier if our frontier has advanced and we confirmed receipt
// gonna need some way to clean stuff up on conn_executor.close()

func (r *Reader) run(ctx context.Context) {

	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func (r *Reader) GetRows(ctx context.Context) ([]tree.Datums, error) {
	if r.state != readerStateIdle {
		return nil, errors.New("reader not idle")
	}
	r.inflightBuffer = append(r.inflightBuffer, r.buf...)
	clear(r.buf)
	r.state = readerStateHasUncommittedBatch
	return r.inflightBuffer, nil

	// and then trigger the goro to check if m wants us to change assignments
	// if it does, handle that stuff before doing a new batch
}

func (r *Reader) ConfirmReceipt(ctx context.Context) error {
	r.state = readerStateIdle
	clear(r.inflightBuffer)

	return nil
}
