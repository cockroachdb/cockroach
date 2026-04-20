// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// DropVectorizer removes the automatic embedding generator from a table.
func (p *planner) DropVectorizer(ctx context.Context, n *tree.DropVectorizer) (planNode, error) {
	return nil, errors.New("DROP VECTORIZER is not yet implemented")
}
