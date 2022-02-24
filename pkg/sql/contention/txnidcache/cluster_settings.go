// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import "github.com/cockroachdb/cockroach/pkg/settings"

// MaxSize limits the maximum byte size can be used by the TxnIDCache.
var MaxSize = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	`sql.contention.txn_id_cache.max_size`,
	"the maximum byte size TxnID cache will use (set to 0 to disable)",
	// See https://github.com/cockroachdb/cockroach/issues/76738.
	0,
).WithPublic()
