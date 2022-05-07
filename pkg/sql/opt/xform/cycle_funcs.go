// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import "github.com/cockroachdb/cockroach/pkg/sql/opt/memo"

// EmptySubqueryPrivate returns an empty SubqueryPrivate.
func (c *CustomFuncs) EmptySubqueryPrivate() *memo.SubqueryPrivate {
	return &memo.SubqueryPrivate{}
}
