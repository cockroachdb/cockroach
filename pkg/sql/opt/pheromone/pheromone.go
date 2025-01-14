// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pheromone

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func BuildChildRequired(
	parent memo.RelExpr, required *physical.Pheromone, childIdx int,
) *physical.Pheromone {
	if required.Any() {
		return nil
	}

	if !physical.VisibleToPheromone(parent) {
		return required
	}

	return required.Child(childIdx)
}
