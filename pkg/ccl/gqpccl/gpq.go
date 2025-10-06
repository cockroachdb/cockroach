// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gpqccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/gpq"
)

func init() {
	gpq.CheckClusterSupportsGenericQueryPlans = checkClusterSupportsGenericQueryPlans
}

func checkClusterSupportsGenericQueryPlans(settings *cluster.Settings) error {
	return utilccl.CheckEnterpriseEnabled(settings, "generic query plans")
}
