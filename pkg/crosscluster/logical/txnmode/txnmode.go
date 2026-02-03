package txnmode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type WriteSet struct {
	Timestamp hlc.Timestamp
	Rows      []streampb.StreamEvent_KV
}

type OrderedFeed interface {
	Next(ctx context.Context) (WriteSet, error)
}

type LockSet struct {
	WriteLocks []uint64
	ReadLocks  []uint64
}

type LockSynthesizer interface {
	SynthesizeLocks(ctx context.Context, writeSet WriteSet) (LockSet, error)
}

// Scheduler already exists

type TransactionWriter interface {
	WriteTransaction(ctx context.Context, writeSet WriteSet) error	
}
