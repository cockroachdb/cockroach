package queuebase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type Manager interface {
	GetOrInitReader(ctx context.Context, name string) (Reader, error)
	CreateQueue(ctx context.Context, name string, tableID int64) error
}

type Reader interface {
	GetRows(ctx context.Context, limit int) ([]tree.Datums, error)
	ConfirmReceipt(ctx context.Context)
}
