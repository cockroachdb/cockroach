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

// CreateVectorizer creates an automatic embedding generator on a table.
func (p *planner) CreateVectorizer(
	ctx context.Context, n *tree.CreateVectorizer,
) (planNode, error) {
	return nil, errors.New("CREATE VECTORIZER is not yet implemented")
}
