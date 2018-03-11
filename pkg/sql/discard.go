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

// Discard implements the DISCARD statement.
// See https://www.postgresql.org/docs/9.6/static/sql-discard.html for details.
func (p *planner) Discard(ctx context.Context, s *tree.Discard) (planNode, error) {
	switch s.Mode {
	case tree.DiscardModeAll:
		if !p.autoCommit {
			return nil, pgerror.NewError(pgerror.CodeActiveSQLTransactionError,
				"DISCARD ALL cannot run inside a transaction block")
		}

		// RESET ALL
		for _, v := range varGen {
			if v.Reset != nil {
				if err := v.Reset(p.sessionDataMutator); err != nil {
					return nil, err
				}
			}
		}

		// DEALLOCATE ALL
		p.preparedStatements.DeleteAll(ctx)
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
			"unknown mode for DISCARD: %d", s.Mode)
	}
	return newZeroNode(nil /* columns */), nil
}
