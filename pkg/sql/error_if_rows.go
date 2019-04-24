// Copyright 2019 The Cockroach Authors.
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

// errorIfRowsNode wraps another planNode and returns an error if the wrapped
// node produces any rows.
type errorIfRowsNode struct {
	plan planNode

	nexted bool
}

func (n *errorIfRowsNode) startExec(params runParams) error {
	return nil
}

func (n *errorIfRowsNode) Next(params runParams) (bool, error) {
	if n.nexted {
		return false, nil
	}
	n.nexted = true

	ok, err := n.plan.Next(params)
	if err != nil {
		return false, err
	}
	if ok {
		// TODO(yuzefovich): update the error once the optimizer plans this node.
		return false, pgerror.Newf(pgerror.CodeForeignKeyViolationError,
			"foreign key violation: values %s", n.plan.Values())
	}
	return false, nil
}

func (n *errorIfRowsNode) Values() tree.Datums {
	return nil
}

func (n *errorIfRowsNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}
