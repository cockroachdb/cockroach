// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// RollbackPrepared aborts a previously prepared transaction and
// deletes its associated entry from the system.prepared_xacts table.
// This is called from ROLLBACK PREPARED.
// Privileges: DELETE on system.prepared_xacts.
func (p *planner) RollbackPrepared(
	ctx context.Context, n *tree.RollbackPrepared,
) (planNode, error) {
	pn, err := p.commitPreparedNode(ctx, n.Transaction, false /* commit */)
	if err != nil {
		return nil, err
	}
	return pn, nil
}
