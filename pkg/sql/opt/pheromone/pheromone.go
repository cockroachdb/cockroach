// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pheromone

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func CanProvide(expr memo.RelExpr, required physical.Pheromone) bool {
	if required.Any() {
		return true
	}
	if expr.Op() != required.Op {
		return false
	}
	if required.Children != nil && expr.ChildCount() != len(required.Children) {
		return false
	}
	return true
}
