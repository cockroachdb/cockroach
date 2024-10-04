// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	gosql "database/sql"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

// ClusterConstructor is used to construct a Cluster for an individual case run.
type ClusterConstructor func(testing.TB) *Cluster

// MakeClusterConstructor creates a new ClusterConstructor using the provided
// function. The intention is that the caller will use the provided knobs when
// constructing the cluster and will return a handle to the SQL database.
func MakeClusterConstructor(
	f func(testing.TB, base.TestingKnobs) (_ *gosql.DB, cleanup func()),
) ClusterConstructor {
	return func(t testing.TB) *Cluster {
		c := &Cluster{}
		beforePlan := func(trace tracingpb.Recording, stmt string) {
			if _, ok := c.stmtToKVBatchRequests.Load(stmt); ok {
				c.stmtToKVBatchRequests.Store(stmt, trace)
			}
		}
		c.sql, c.cleanup = f(t, base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				WithStatementTrace: beforePlan,
			},
		})
		return c
	}
}

// Cluster abstracts a cockroach cluster for use in rttanalysis benchmarks.
type Cluster struct {
	stmtToKVBatchRequests sync.Map
	cleanup               func()
	sql                   *gosql.DB
}

func (c *Cluster) conn() *gosql.DB {
	return c.sql
}

func (c *Cluster) clearStatementTrace(stmt string) {
	c.stmtToKVBatchRequests.Store(stmt, nil)
}

func (c *Cluster) getStatementTrace(stmt string) (tracingpb.Recording, bool) {
	out, _ := c.stmtToKVBatchRequests.Load(stmt)
	r, ok := out.(tracingpb.Recording)
	return r, ok
}

func (c *Cluster) close() {
	c.cleanup()
}
