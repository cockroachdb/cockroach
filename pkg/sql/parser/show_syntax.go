// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parser

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
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
	if strings.HasSuffix(stmt, "??") {
		// A statement (or, more likely, a prefix to a statement) followed
		// by the help token (??).
		//
		// In that case, take a shortcut to avoid the complexity of
		// instantiating a whole parser just to retrieve a help string.
		// This also has the benefit of supporting retrieving help for
		// the special non-terminals e.g. "<source> ??".
		prefix := strings.ToUpper(strings.TrimSpace(stmt[:len(stmt)-2]))
		if h, ok := HelpMessages[prefix]; ok {
			msg := HelpMessage{Command: prefix, HelpMessageBody: h}
			msgs := msg.String()
			err := errors.WithHint(pgerror.WithCandidateCode(errors.New(specialHelpErrorPrefix), pgcode.Syntax), msgs)
			doErr(ctx, report, reportErr, err)
			return
		}
	}

	stmts, err := Parse(stmt)
	if err != nil {
		doErr(ctx, report, reportErr, err)
	} else {
		for i := range stmts {
			report(ctx, "sql", tree.AsStringWithFlags(stmts[i].AST, tree.FmtParsable))
		}
	}
}

func doErr(
	ctx context.Context,
	report func(ctx context.Context, field, msg string),
	reportErr func(ctx context.Context, err error),
	err error,
) {
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
}
