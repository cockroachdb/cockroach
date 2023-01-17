// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostserver

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type instance struct {
	db         *kv.DB
	ief        isql.DB
	metrics    Metrics
	timeSource timeutil.TimeSource
	settings   *cluster.Settings
}

// Note: the "four" in the description comes from
//
//	tenantcostclient.extendedReportingPeriodFactor.
var instanceInactivity = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"tenant_usage_instance_inactivity",
	"instances that have not reported consumption for longer than this value are cleaned up; "+
		"should be at least four times higher than the tenant_cost_control_period of any tenant",
	1*time.Minute, settings.PositiveDuration,
)

func newInstance(
	settings *cluster.Settings, db *kv.DB, ief isql.DB, timeSource timeutil.TimeSource,
) *instance {
	res := &instance{
		db:         db,
		ief:        ief,
		timeSource: timeSource,
		settings:   settings,
	}
	res.metrics.init()
	return res
}

// Metrics is part of the multitenant.TenantUsageServer.
func (s *instance) Metrics() metric.Struct {
	return &s.metrics
}

var _ multitenant.TenantUsageServer = (*instance)(nil)

func init() {
	server.NewTenantUsageServer = func(
		settings *cluster.Settings,
		db *kv.DB,
		ief isql.DB,
	) multitenant.TenantUsageServer {
		return newInstance(settings, db, ief, timeutil.DefaultTimeSource{})
	}
}
