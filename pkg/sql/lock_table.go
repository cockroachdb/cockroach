// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) LockTable(ctx context.Context, n *tree.LockTable) (planNode, error) {
	if p.SessionData().PgDumpCompatibility {
		// CockroachDB uses MVCC, so all lock modes are implicitly satisfied
		// by the concurrency control system. This is a no-op for pg_dump
		// compatibility.
		return newZeroNode(nil /* columns */), nil
	}
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"LOCK TABLE is not supported")
}
