// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// EmptyExistsPrivate returns an empty SubqueryPrivate.
func (c *CustomFuncs) EmptyExistsPrivate() *memo.ExistsPrivate {
	col := c.e.f.Metadata().AddColumn("exists", types.Bool)
	return &memo.ExistsPrivate{LazyEvalProjectionCol: col}
}
