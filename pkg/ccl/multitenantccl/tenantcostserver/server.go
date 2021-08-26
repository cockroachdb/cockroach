// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type instance struct {
	db         *kv.DB
	executor   *sql.InternalExecutor
	metrics    Metrics
	timeSource timeutil.TimeSource
}

func newInstance(
	db *kv.DB, executor *sql.InternalExecutor, timeSource timeutil.TimeSource,
) *instance {
	res := &instance{
		db:         db,
		executor:   executor,
		timeSource: timeSource,
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
		db *kv.DB, executor *sql.InternalExecutor,
	) multitenant.TenantUsageServer {
		return newInstance(db, executor, timeutil.DefaultTimeSource{})
	}
}
