// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/redact"
)

var redactNamesInSQLStatementLog = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.log.redact_names.enabled",
	"if set, schema object identifers are redacted in SQL statements that appear in event logs",
	false,
	settings.WithPublic,
)

// FormatAstAsRedactableString implements scbuild.AstFormatter
func FormatAstAsRedactableString(
	statement Statement, annotations *Annotations, sv *settings.Values,
) redact.RedactableString {
	fs := FmtSimple | FmtAlwaysQualifyTableNames | FmtMarkRedactionNode
	if !redactNamesInSQLStatementLog.Get(sv) {
		fs = fs | FmtOmitNameRedaction
	}
	return formatStmtKeyAsRedactableString(statement, annotations, fs)
}

// formatStmtKeyAsRedactableString given an AST node this function will fully
// qualify names using annotations to format it out into a redactable string.
// Object names are not redacted, but constants and datums are.
func formatStmtKeyAsRedactableString(
	rootAST Statement, ann *Annotations, fs FmtFlags,
) redact.RedactableString {
	f := NewFmtCtx(
		fs,
		FmtAnnotations(ann),
	)
	f.FormatNode(rootAST)
	formattedRedactableStatementString := f.CloseAndGetString()
	return redact.RedactableString(formattedRedactableStatementString)
}
