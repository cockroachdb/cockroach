package queuebase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type Manager interface {
	CreateQueue(ctx context.Context, name string, tableID int64) error
}

// Implemented by the conn executor in reality
type ReaderProvider interface {
	GetOrInitReader(ctx context.Context, name string) (Reader, error)
}

type Reader interface {
	GetRows(ctx context.Context, limit int) ([]tree.Datums, error)
	ConfirmReceipt(ctx context.Context)
	RollbackBatch(ctx context.Context)
	Close() error
}
