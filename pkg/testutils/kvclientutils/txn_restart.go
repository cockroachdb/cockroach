// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvclientutils

import (
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

var randRetryRngSource, _ = randutil.NewLockedPseudoRand()

func RandomTransactionRetryFilter() func(*kv.Txn) bool {
	return func(*kv.Txn) bool {
		return randRetryRngSource.Float64() < kv.RandomTxnRetryProbability
	}
}
