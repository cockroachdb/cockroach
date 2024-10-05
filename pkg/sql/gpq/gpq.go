// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gpq

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

var CheckClusterSupportsGenericQueryPlans = func(settings *cluster.Settings, cluster uuid.UUID) error {
	return sqlerrors.NewCCLRequiredError(
		errors.New("plan_cache_mode=force_generic_plan and plan_cache_mode=auto require a CCL binary"),
	)
}
