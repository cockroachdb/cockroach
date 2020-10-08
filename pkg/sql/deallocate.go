// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Deallocate implements the DEALLOCATE statement.
// See https://www.postgresql.org/docs/current/static/sql-deallocate.html for details.
func (p *planner) Deallocate(ctx context.Context, s *tree.Deallocate) (planNode, error) {
	if s.Name == "" {
		p.preparedStatements.DeleteAll(ctx)
	} else {
		if found := p.preparedStatements.Delete(ctx, string(s.Name)); !found {
			return nil, pgerror.Newf(pgcode.InvalidSQLStatementName,
				"prepared statement %q does not exist", s.Name)
		}
	}
	return newZeroNode(nil /* columns */), nil
}
