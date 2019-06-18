// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
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
		return false, pgerror.Newf(pgcode.ForeignKeyViolation,
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
