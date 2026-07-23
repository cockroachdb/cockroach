// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/compengine"
	"github.com/cockroachdb/cockroach/pkg/sql/comprules"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// completionsNode is a shim planNode around a completionsGenerator.
// The "main" logic is in completionsGenerator, which is also
// used without a planNode by the connExecutor when running SHOW
// COMPLETIONS as an observer statement.
type completionsNode struct {
	zeroInputPlanNode
	optColumnsSlot

	n       *tree.ShowCompletions
	results compengine.Engine
}

func (n *completionsNode) startExec(params runParams) (err error) {
	override := sessiondata.InternalExecutorOverride{User: params.p.User()}
	queryIterFn := func(ctx context.Context, opName redact.RedactableString, stmt string, args ...interface{}) (compengine.Rows, error) {
		return params.p.QueryIteratorEx(ctx, opName,
			override,
			stmt, args...)
	}
	n.results, err = newCompletionsGenerator(queryIterFn, n.n)
	return err
}

func (n *completionsNode) Next(params runParams) (bool, error) {
	return n.results.Next(params.ctx)
}

func (n *completionsNode) Values() tree.Datums {
	return n.results.Values()
}

func (n *completionsNode) Close(ctx context.Context) {
	if n.results == nil {
		return
	}
	n.results.Close(ctx)
	n.results = nil
}

func newCompletionsGenerator(
	queryIter compengine.QueryIterFn, sc *tree.ShowCompletions,
) (compengine.Engine, error) {
	offsetVal, ok := sc.Offset.AsConstantInt()
	if !ok {
		return nil, errors.Newf("invalid offset %v", sc.Offset)
	}
	offset, err := strconv.Atoi(offsetVal.String())
	if err != nil {
		return nil, err
	}
	input := sc.Statement.RawString()
	return compengine.New(queryIter, comprules.GetCompMethods(), offset, input), nil
}
