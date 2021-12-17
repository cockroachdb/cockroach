// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// DeclareCursor implements the DECLARE statement.
// See https://www.postgresql.org/docs/current/sql-declare.html for details.
func (p *planner) DeclareCursor(ctx context.Context, s *tree.DeclareCursor) (planNode, error) {
	return nil, unimplemented.NewWithIssue(41412, "DECLARE CURSOR")
}

// FetchCursor implements the FETCH statement.
// See https://www.postgresql.org/docs/current/sql-fetch.html for details.
func (p *planner) FetchCursor(ctx context.Context, s *tree.FetchCursor) (planNode, error) {
	return nil, unimplemented.NewWithIssue(41412, "FETCH CURSOR")
}

// CloseCursor implements the FETCH statement.
// See https://www.postgresql.org/docs/current/sql-close.html for details.
func (p *planner) CloseCursor(ctx context.Context, s *tree.CloseCursor) (planNode, error) {
	return nil, unimplemented.NewWithIssue(41412, "CLOSE CURSOR")
}
