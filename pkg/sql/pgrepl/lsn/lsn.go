// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package lsn contains logic for handling the pg_lsn type.
package lsn

import (
	"fmt"

	"github.com/cockroachdb/apd/v3"
)

type LSN uint64

func (lsn LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

func ParseLSN(str string) (LSN, error) {
	var lo, hi uint32
	if _, err := fmt.Sscanf(str, "%X/%X", &hi, &lo); err != nil {
		return 0, err
	}
	return (LSN(hi) << 32) | LSN(lo), nil
}

func (lsn LSN) Decimal() (*apd.Decimal, error) {
	ret, _, err := apd.NewFromString(fmt.Sprintf("%d", lsn))
	return ret, err
}

func (lsn LSN) Compare(other LSN) int {
	if lsn > other {
		return 1
	}
	if lsn < other {
		return -1
	}
	return 0
}

func (lsn LSN) Add(val LSN) LSN {
	return lsn + val
}

func (lsn LSN) Sub(val LSN) LSN {
	return lsn - val
}
