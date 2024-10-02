// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Deallocate implements the DEALLOCATE statement.
// See https://www.postgresql.org/docs/current/static/sql-deallocate.html for details.
func (p *planner) Deallocate(ctx context.Context, s *tree.Deallocate) (planNode, error) {
	if s.Name == "" {
		p.preparedStatements.DeleteAll(ctx)
	} else {
		if found := p.preparedStatements.Delete(ctx, string(s.Name)); !found {
			return nil, newPreparedStmtDNEError(p.SessionData(), string(s.Name))
		}
	}
	return newZeroNode(nil /* columns */), nil
}
