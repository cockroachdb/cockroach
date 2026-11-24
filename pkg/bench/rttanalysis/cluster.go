// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

// ClusterConstructor is used to construct a Cluster for an individual case run.
type ClusterConstructor func(tb testing.TB, measureRoundtrips bool) *Cluster

// MakeClusterConstructor creates a new ClusterConstructor using the provided
// function. The intention is that the caller will use the provided knobs when
// constructing the cluster and will return a handle to the SQL database.
func MakeClusterConstructor(
	f func(testing.TB, base.TestingKnobs) (_, _ *gosql.DB, cleanup func()),
) ClusterConstructor {
	return func(t testing.TB, measureRoundtrips bool) *Cluster {
		c := &Cluster{}
		beforePlan := func(trace tracingpb.Recording, stmt string) {
			c.stmtToKVBatchRequests.Store(stmt, &trace)
		}
		knobs := base.TestingKnobs{}
		if measureRoundtrips {
			knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
				WithStatementTrace: beforePlan,
			}
		}
		c.adminSQLConn, c.nonAdminSQLConn, c.cleanup = f(t, knobs)
		return c
	}
}

// Cluster abstracts a cockroach cluster for use in rttanalysis benchmarks.
type Cluster struct {
	stmtToKVBatchRequests syncutil.Map[string, tracingpb.Recording]
	cleanup               func()

	// adminSQLConn should be the default connection for tests. It specifies a
	// user with admin privileges.
	adminSQLConn *gosql.DB

	// nonAdminSQLConn should be used for tests that require a non-admin user.
	// It may be unset for tests that don't need it.
	nonAdminSQLConn *gosql.DB
}

func (c *Cluster) adminConn() *gosql.DB {
	return c.adminSQLConn
}

func (c *Cluster) nonAdminConn() *gosql.DB {
	return c.nonAdminSQLConn
}

func (c *Cluster) clearStatementTrace(stmt string) {
	c.stmtToKVBatchRequests.Delete(stmt)
}

func (c *Cluster) getStatementTrace(stmt string) (tracingpb.Recording, bool) {
	if out, ok := c.stmtToKVBatchRequests.Load(stmt); ok {
		return *out, true
	}
	return tracingpb.Recording{}, false
}

func (c *Cluster) close() {
	c.cleanup()
}
