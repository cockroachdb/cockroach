// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"

func registerAdmission(r registry.Registry) {
	registerMultiStoreOverload(r)
	registerTPCCOverload(r)
	// TODO(irfansharif): Once registerMultiTenantFairness is unskipped and
	// observed to be non-flaky for 3-ish months, transfer ownership to the AC
	// group + re-home it here.
}
