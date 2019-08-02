// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

type opaqueMetadata struct {
	info string
	plan planNode
}

var _ opt.OpaqueMetadata = &opaqueMetadata{}

func (o *opaqueMetadata) ImplementsOpaqueMetadata() {}
func (o *opaqueMetadata) String() string            { return o.info }

func buildOpaque(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, stmt tree.Statement,
) (opt.OpaqueMetadata, sqlbase.ResultColumns, error) {
	p := evalCtx.Planner.(*planner)

	var plan planNode
	var err error
	switch n := stmt.(type) {
	case *tree.ShowClusterSetting:
		plan, err = p.ShowClusterSetting(ctx, n)

	case *tree.ShowHistogram:
		plan, err = p.ShowHistogram(ctx, n)

	case *tree.ShowTableStats:
		plan, err = p.ShowTableStats(ctx, n)

	case *tree.ShowTraceForSession:
		plan, err = p.ShowTrace(ctx, n)

	case *tree.ShowZoneConfig:
		plan, err = p.ShowZoneConfig(ctx, n)

	case *tree.ShowFingerprints:
		plan, err = p.ShowFingerprints(ctx, n)

	default:
		return nil, nil, errors.AssertionFailedf("unknown opaque statement %T", stmt)
	}
	if err != nil {
		return nil, nil, err
	}
	res := &opaqueMetadata{
		info: stmt.StatementTag(),
		plan: plan,
	}
	return res, planColumns(plan), nil
}

func init() {
	opaqueReadOnlyStmts := []reflect.Type{
		reflect.TypeOf(&tree.ShowClusterSetting{}),
		reflect.TypeOf(&tree.ShowHistogram{}),
		reflect.TypeOf(&tree.ShowTableStats{}),
		reflect.TypeOf(&tree.ShowTraceForSession{}),
		reflect.TypeOf(&tree.ShowZoneConfig{}),
		reflect.TypeOf(&tree.ShowFingerprints{}),
	}
	for _, t := range opaqueReadOnlyStmts {
		optbuilder.RegisterOpaque(t, optbuilder.OpaqueReadOnly, buildOpaque)
	}
}
