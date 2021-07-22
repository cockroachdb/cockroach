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
)

type instance struct {
	db       *kv.DB
	executor *sql.InternalExecutor
}

func newInstance(db *kv.DB, executor *sql.InternalExecutor) *instance {
	return &instance{
		db:       db,
		executor: executor,
	}
}

var _ multitenant.TenantUsageServer = (*instance)(nil)

func init() {
	server.NewTenantUsageServer = func(
		db *kv.DB, executor *sql.InternalExecutor,
	) multitenant.TenantUsageServer {
		return newInstance(db, executor)
	}
}
