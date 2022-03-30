// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import "github.com/cockroachdb/cockroach/pkg/settings"

// AssociateStmtWithTxnFingerprint determines whether to segment
// per-statment statistics by transaction fingerprint. While enabled by
// default, it may be useful to disable for workloads that run the same
// statements across many (ad-hoc) transaction fingerprints, producing
// higher-cardinality data in the system.statement_statistics table than
// the cleanup job is able to keep up with. See #78338.
var AssociateStmtWithTxnFingerprint = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stats.associate_stmt_with_txn_fingerprint.enabled",
	"whether to segment per-statement query statistics by transaction fingerprint",
	true,
)
