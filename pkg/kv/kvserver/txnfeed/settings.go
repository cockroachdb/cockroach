// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"

// Enabled is an alias for kvserverbase.TxnFeedEnabled, kept here for
// convenience so that callers within kvserver can continue to use
// txnfeed.Enabled without changing their import paths.
var Enabled = kvserverbase.TxnFeedEnabled
