// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// RunShowSyntax analyzes the syntax and reports its structure as data
// for the client. Even an error is reported as data.
//
// Since errors won't propagate to the client as an error, but as
// a result, the usual code path to capture and record errors will not
// be triggered. Instead, the caller can pass a reportErr closure to
// capture errors instead. May be nil.
func RunShowSyntax(
	ctx context.Context,
	stmt string,
	report func(ctx context.Context, field, msg string),
	reportErr func(ctx context.Context, err error),
) {
	stmts, err := Parse(stmt)
	if err != nil {
		if reportErr != nil {
			reportErr(ctx, err)
		}

		pqErr := pgerror.Flatten(err)
		report(ctx, "error", pqErr.Message)
		report(ctx, "code", pqErr.Code)
		if pqErr.Source != nil {
			if pqErr.Source.File != "" {
				report(ctx, "file", pqErr.Source.File)
			}
			if pqErr.Source.Line > 0 {
				report(ctx, "line", fmt.Sprintf("%d", pqErr.Source.Line))
			}
			if pqErr.Source.Function != "" {
				report(ctx, "function", pqErr.Source.Function)
			}
		}
		if pqErr.Detail != "" {
			report(ctx, "detail", pqErr.Detail)
		}
		if pqErr.Hint != "" {
			report(ctx, "hint", pqErr.Hint)
		}
	} else {
		for i := range stmts {
			report(ctx, "sql", tree.AsStringWithFlags(stmts[i].AST, tree.FmtParsable))
		}
	}
}
