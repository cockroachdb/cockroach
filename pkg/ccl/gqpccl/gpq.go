// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package gpqccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/gpq"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	gpq.CheckClusterSupportsGenericQueryPlans = checkClusterSupportsGenericQueryPlans
}

func checkClusterSupportsGenericQueryPlans(settings *cluster.Settings, cluster uuid.UUID) error {
	return utilccl.CheckEnterpriseEnabled(settings, cluster, "generic query plans")
}
