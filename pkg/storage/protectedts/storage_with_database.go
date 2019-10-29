package protectedts

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// WithDatabase wraps s such that any calls made with a nil *Txn will be wrapped
// in a call to db.Txn. This is often convenient in testing.
func WithDatabase(s Storage, db *client.DB) Storage {
	return &storageWithDatabase{s: s, db: db}
}

type storageWithDatabase struct {
	db *client.DB
	s  Storage
}

func (s *storageWithDatabase) Protect(ctx context.Context, txn *client.Txn, r *ptpb.Record) error {
	if txn == nil {
		return s.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			return s.s.Protect(ctx, txn, r)
		})
	}
	return s.s.Protect(ctx, txn, r)
}

func (s *storageWithDatabase) Release(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	if txn == nil {
		return s.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			return s.s.Release(ctx, txn, id)
		})
	}
	return s.s.Release(ctx, txn, id)
}

func (s *storageWithDatabase) GetRecord(
	ctx context.Context, txn *client.Txn, id uuid.UUID,
) (r *ptpb.Record, createdAt hlc.Timestamp, err error) {
	if txn == nil {
		err = s.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			r, createdAt, err = s.s.GetRecord(ctx, txn, id)
			return err
		})
		return r, createdAt, err
	}
	return s.s.GetRecord(ctx, txn, id)
}

func (s *storageWithDatabase) GetMetadata(
	ctx context.Context, txn *client.Txn,
) (md ptpb.Metadata, err error) {
	if txn == nil {
		err = s.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			md, err = s.s.GetMetadata(ctx, txn)
			return err
		})
		return md, err
	}
	return s.s.GetMetadata(ctx, txn)
}

func (s *storageWithDatabase) GetState(
	ctx context.Context, txn *client.Txn,
) (state ptpb.State, err error) {
	if txn == nil {
		err = s.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			state, err = s.s.GetState(ctx, txn)
			return err
		})
		return state, err
	}
	return s.s.GetState(ctx, txn)
}
