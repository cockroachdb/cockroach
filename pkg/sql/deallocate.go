// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

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
			return nil, pgerror.NewErrorf(pgerror.CodeInvalidSQLStatementNameError,
				"prepared statement %q does not exist", s.Name)
		}
	}
	return newZeroNode(nil /* columns */), nil
}
