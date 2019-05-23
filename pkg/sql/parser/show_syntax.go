// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	report func(ctx context.Context, field, msg string) error,
	reportErr func(ctx context.Context, err error),
) error {
	stmts, err := Parse(stmt)
	if err != nil {
		if reportErr != nil {
			reportErr(ctx, err)
		}

		pqErr := pgerror.Flatten(err)
		if err := report(ctx, "error", pqErr.Message); err != nil {
			return err
		}
		if err := report(ctx, "code", pqErr.Code); err != nil {
			return err
		}
		if pqErr.Source != nil {
			if pqErr.Source.File != "" {
				if err := report(ctx, "file", pqErr.Source.File); err != nil {
					return err
				}
			}
			if pqErr.Source.Line > 0 {
				if err := report(ctx, "line", fmt.Sprintf("%d", pqErr.Source.Line)); err != nil {
					return err
				}
			}
			if pqErr.Source.Function != "" {
				if err := report(ctx, "function", pqErr.Source.Function); err != nil {
					return err
				}
			}
		}
		if pqErr.Detail != "" {
			if err := report(ctx, "detail", pqErr.Detail); err != nil {
				return err
			}
		}
		if pqErr.Hint != "" {
			if err := report(ctx, "hint", pqErr.Hint); err != nil {
				return err
			}
		}
	} else {
		for i := range stmts {
			str := tree.AsStringWithFlags(stmts[i].AST, tree.FmtParsable)
			if err := report(ctx, "sql", str); err != nil {
				return err
			}
		}
	}
	return nil
}
