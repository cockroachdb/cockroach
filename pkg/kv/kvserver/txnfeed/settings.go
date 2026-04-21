// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import "github.com/cockroachdb/cockroach/pkg/settings"

// Enabled controls whether TxnFeed is active. It gates the TxnFeed
// streaming RPC, the emission of CommitTxnOp events through Raft, and
// the writing of committed transaction records to MVCC history for
// catch-up scans.
var Enabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.txnfeed.enabled",
	"if set, the TxnFeed streaming RPC for committed transaction records is enabled",
	false,
)
