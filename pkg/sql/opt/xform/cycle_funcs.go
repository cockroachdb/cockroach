// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package xform

import "github.com/cockroachdb/cockroach/pkg/sql/opt/memo"

// EmptySubqueryPrivate returns an empty SubqueryPrivate.
func (c *CustomFuncs) EmptySubqueryPrivate() *memo.SubqueryPrivate {
	return &memo.SubqueryPrivate{}
}
