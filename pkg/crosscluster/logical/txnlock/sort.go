// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
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

// sort topologically sorts the write set into the order writes should be
// applied.
func (ls *LockSynthesizer) sort(
	ctx context.Context,
	rows []ldrdecoder.DecodedRow,
	rowLocks [][]Lock,
	locks map[LockHash]rowsWithLock,
) ([]int, error) {
	status := make([]sortStatus, len(rows))
	order := make([]int, 0, len(rows))

	for i := range rows {
		rowOrder, err := ls.sortInner(ctx, i, rows, status, rowLocks, locks)
		if err != nil {
			return nil, err
		}
		order = append(order, rowOrder...)
	}

	return order, nil
}

func (ls *LockSynthesizer) sortInner(
	ctx context.Context,
	row int,
	rows []ldrdecoder.DecodedRow,
	status []sortStatus,
	rowLocks [][]Lock,
	locks map[LockHash]rowsWithLock,
) ([]int, error) {
	if status[row] == statusVisited {
		return nil, nil
	}
	if status[row] == statusVisiting {
		return nil, ErrApplyCycle
	}

	status[row] = statusVisiting

	var rowOrder []int
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
			dep, err := ls.dependsOn(ctx, rows[row], rows[dependentRow])
			if err != nil {
				return nil, err
			}
			if !dep {
				continue
			}
			innerRowOrder, err := ls.sortInner(ctx, int(dependentRow), rows, status, rowLocks, locks)
			if err != nil {
				return nil, err
			}
			rowOrder = append(rowOrder, innerRowOrder...)
		}
	}

	status[row] = statusVisited
	rowOrder = append(rowOrder, row)
	return rowOrder, nil
}
