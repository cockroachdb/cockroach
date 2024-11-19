// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
