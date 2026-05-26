// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnidcache

import "github.com/cockroachdb/cockroach/pkg/settings"

// MaxSize limits the maximum byte size can be used by the TxnIDCache.
var MaxSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	`sql.contention.txn_id_cache.max_size`,
	"the maximum byte size TxnID cache will use (set to 0 to disable)",
	64*1024*1024, // 64MiB
	settings.WithPublic)
