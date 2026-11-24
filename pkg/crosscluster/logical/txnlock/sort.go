// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/errors"
)

// ErrApplyCycle is returned when topological sorting detects a cycle in the
// dependency graph between rows.
var ErrApplyCycle = errors.New("cycle detected in apply order")

type sortStatus int

const (
	statusVisiting sortStatus = 1
	statusVisited  sortStatus = 2
)

// sort topologically sorts the txn envelope by the order writes should be
// applied.
func (ls *LockSynthesizer) sort(
	ctx context.Context,
	envelope ldrdecoder.TxnEnvelope,
	rowLocks [][]Lock,
	locks map[LockHash]rowsWithLock,
) (ldrdecoder.TxnEnvelope, error) {
	n := len(envelope.Txn.WriteSet)
	status := make([]sortStatus, n)
	sorted := ldrdecoder.TxnEnvelope{
		Txn: ldrdecoder.Transaction{
			TxnID:    envelope.Txn.TxnID,
			WriteSet: make([]ldrdecoder.DecodedRow, 0, n),
		},
		RawKVs: make([]streampb.StreamEvent_KV, 0, n),
	}

	for i := range envelope.Txn.WriteSet {
		if err := ls.sortInner(ctx, i, envelope, status, rowLocks, locks, &sorted); err != nil {
			return ldrdecoder.TxnEnvelope{}, err
		}
	}

	return sorted, nil
}

func (ls *LockSynthesizer) sortInner(
	ctx context.Context,
	row int,
	envelope ldrdecoder.TxnEnvelope,
	status []sortStatus,
	rowLocks [][]Lock,
	locks map[LockHash]rowsWithLock,
	output *ldrdecoder.TxnEnvelope,
) error {
	if status[row] == statusVisited {
		return nil
	}
	if status[row] == statusVisiting {
		return ErrApplyCycle
	}

	status[row] = statusVisiting

	// This is mostly a standard topological sort with one quirk. We are using
	// the lock set to identify possible dependencies. So we need to additionally
	// filter with dependsOn to ensure there is a real edge between the rows. If
	// we allow for hash conflicts here, we would reject the transaction as
	// containing a cycle when we can actually apply it.
	//
	// For most of LDR, its okay if we have a lock hash conflict because it
	// results in a spurious dependency between transactions. It can't result in
	// a cycle because one of the transactions must come first in mvcc time.
	for _, lock := range rowLocks[row] {
		lr := locks[lock.Hash]
		for _, dependentRow := range lr.rows {
			if dependentRow == int32(row) {
				continue
			}
			dep, err := ls.dependsOn(ctx, envelope.Txn.WriteSet[row], envelope.Txn.WriteSet[dependentRow])
			if err != nil {
				return err
			}
			if !dep {
				continue
			}
			if err := ls.sortInner(ctx, int(dependentRow), envelope, status, rowLocks, locks, output); err != nil {
				return err
			}
		}
	}

	status[row] = statusVisited
	output.Txn.WriteSet = append(output.Txn.WriteSet, envelope.Txn.WriteSet[row])
	output.RawKVs = append(output.RawKVs, envelope.RawKVs[row])
	return nil
}
