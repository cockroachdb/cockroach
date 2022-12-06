// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package comprules

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/compengine"
)

func method(
	label string, fn func(context.Context, compengine.Context) (compengine.Rows, error),
) compengine.Method {
	return m{label, fn}
}

type m struct {
	label string
	fn    func(context.Context, compengine.Context) (compengine.Rows, error)
}

var _ compengine.Method = m{}

func (m m) Name() string { return m.label }
func (m m) Call(ctx context.Context, c compengine.Context) (compengine.Rows, error) {
	return m.fn(ctx, c)
}
