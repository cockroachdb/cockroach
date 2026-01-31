// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/errors"
)

var ApplyCycle = errors.New("cycle detected in apply order")

type sortStatus int

const (
	statusVisiting sortStatus = 1
	statusVisited  sortStatus = 2
)

func (ls *LockSynthesizer) sort(
	rows []ldrdecoder.DecodedRow, rowLocks [][]Lock, locks map[uint64]lockWithRows,
) ([]ldrdecoder.DecodedRow, error) {
	status := make([]sortStatus, len(rows))
	sorted := make([]ldrdecoder.DecodedRow, 0, len(rows))

	// We iterate backwards so that the order is stable with respect to the input
	// if there are no dependencies.
	for i := range rows {
		err := ls.sortInner(i, rows, status, rowLocks, locks, &sorted)
		if err != nil {
			return nil, err
		}
	}

	return sorted, nil
}

func (ls *LockSynthesizer) sortInner(
	row int,
	rows []ldrdecoder.DecodedRow,
	status []sortStatus,
	rowLocks [][]Lock,
	locks map[uint64]lockWithRows,
	output *[]ldrdecoder.DecodedRow,
) error {
	if status[row] == statusVisited {
		return nil
	}
	if status[row] == statusVisiting {
		return ApplyCycle
	}

	status[row] = statusVisiting

	for _, lock := range rowLocks[row] {
		lr := locks[lock.Hash]
		for _, dependentRow := range lr.rows {
			if dependentRow == int32(row) {
				continue
			}
			if !ls.dependsOn(rows[row], rows[dependentRow]) {
				continue
			}
			if err := ls.sortInner(int(dependentRow), rows, status, rowLocks, locks, output); err != nil {
				return err
			}
		}
	}

	status[row] = statusVisited
	*output = append(*output, rows[row])
	return nil
}
