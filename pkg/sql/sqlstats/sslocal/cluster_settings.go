// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sslocal

import "github.com/cockroachdb/cockroach/pkg/settings"

// AssociateStmtWithTxnFingerprint determines whether to segment
// per-statment statistics by transaction fingerprint. While enabled by
// default, it may be useful to disable for workloads that run the same
// statements across many (ad-hoc) transaction fingerprints, producing
// higher-cardinality data in the system.statement_statistics table than
// the cleanup job is able to keep up with. See #78338.
var AssociateStmtWithTxnFingerprint = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.stats.associate_stmt_with_txn_fingerprint.enabled",
	"whether to segment per-statement query statistics by transaction fingerprint",
	true,
)
