// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import "context"

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
