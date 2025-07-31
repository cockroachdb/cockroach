// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql"
)

func init() {
	plpgsql.CheckClusterSupportsPLpgSQL = checkClusterSupportsPLpgSQL
}

func checkClusterSupportsPLpgSQL(settings *cluster.Settings) error {
	return utilccl.CheckEnterpriseEnabled(settings, "PL/pgSQL")
}
