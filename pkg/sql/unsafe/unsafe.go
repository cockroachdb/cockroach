// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unsafe

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/redact"
)

// HasUnsafeInternalsAccess checks if the current session has permission to access
// unsafe internal tables and functionality. This includes system tables and
// virtual tables in the crdb_internal schema.
func HasUnsafeInternalsAccess(
	ctx context.Context,
	actx *log.AmbientContext,
	sd *sessiondata.SessionData,
	stmt tree.Statement,
	ann *tree.Annotations,
) error {
	// If the querier is internal, we should allow it.
	if sd.Internal {
		return nil
	}

	// If an override is set, allow access to this virtual table.
	if sd.AllowUnsafeInternals {
		// As this is considered a "broken glass" situation, we report the access to the event log.
		log.EventLog(ctx, actx, &eventpb.UnsafeTableAccess{Query: formatStmtKeyAsRedactableString(stmt, ann)})
		return nil
	}

	log.SqlExec.Errorf(ctx, "unsafe table access attempted: %s", stmt)
	return sqlerrors.ErrUnsafeTableAccess
}

// formatStmtKeyAsRedactableString given an AST node this function will fully
// qualify names using annotations to format it out into a redactable string.
// Object names are not redacted, but constants and datums are.
func formatStmtKeyAsRedactableString(
	rootAST tree.Statement, ann *tree.Annotations,
) redact.RedactableString {
	fs := tree.FmtSimple | tree.FmtAlwaysQualifyTableNames | tree.FmtMarkRedactionNode
	f := tree.NewFmtCtx(
		fs,
		tree.FmtAnnotations(ann),
	)
	f.FormatNode(rootAST)
	formattedRedactableStatementString := f.CloseAndGetString()
	return redact.RedactableString(formattedRedactableStatementString)
}
