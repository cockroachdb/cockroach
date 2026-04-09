// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnscheduler

import (
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnlock"
)

// Transaction contains all of the information about a transaction needed to
// identify its dependencies.
type Transaction struct {
	TxnID ldrdecoder.TxnID
	Locks []txnlock.Lock
}
