// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package plpgsqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	plpgsql.CheckClusterSupportsPLpgSQL = checkClusterSupportsPLpgSQL
}

func checkClusterSupportsPLpgSQL(settings *cluster.Settings, _ uuid.UUID) error {
	return utilccl.CheckEnterpriseEnabled(settings, "PL/pgSQL")
}
