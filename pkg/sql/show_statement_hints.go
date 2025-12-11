// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// ShowStatementHints returns a SHOW STATEMENT HINTS statement.
func (p *planner) ShowStatementHints(
	ctx context.Context, n *tree.ShowStatementHints,
) (planNode, error) {
	hasPrivileges, err := p.HasPrivilege(
		ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA, p.User(),
	)
	if err != nil {
		return nil, err
	} else if !hasPrivileges {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"must have %s privilege to run `SHOW STATEMENT HINTS`",
			privilege.VIEWCLUSTERMETADATA.DisplayName(),
		)
	}

	sqltelemetry.IncrementShowCounter(sqltelemetry.StatementHints)

	// Analyze the expression during planning to ensure it's valid, but don't
	// evaluate it yet - that must happen at execution time for prepared
	// statements.
	//
	// Note: it is probably not technically necessary to call analyzeExpr
	// currently since we're only allowing string literals and placeholders, but
	// we do it anyway to be future-proof in case we want to allow more complex
	// expressions later.
	typedExpr, err := p.analyzeExpr(
		ctx, n.Expr, tree.IndexedVarHelper{}, types.String, true, "SHOW STATEMENT HINTS",
	)
	if err != nil {
		return nil, err
	}

	columns := colinfo.ShowStatementHintsColumns
	if n.Options.Details {
		columns = colinfo.ShowStatementHintsDetailsColumns
	}

	return &delayedNode{
		name:    n.StatementTag(),
		columns: columns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			// Evaluate the expression to get the SQL string at execution time.
			sqlStr, err := eval.Expr(ctx, p.EvalContext(), typedExpr)
			if err != nil {
				return nil, err
			}
			if sqlStr == tree.DNull {
				return nil, pgerror.New(pgcode.NullValueNotAllowed,
					"SHOW STATEMENT HINTS requires a non-null string")
			}
			sqlString := string(tree.MustBeDString(sqlStr))

			// Parse the SQL string to get a Statement.
			stmts, err := parser.Parse(sqlString)
			if err != nil {
				return nil, err
			}
			if len(stmts) == 0 {
				return nil, pgerror.New(pgcode.Syntax,
					"SHOW STATEMENT HINTS requires a non-empty SQL string")
			}
			if len(stmts) > 1 {
				return nil, pgerror.Newf(pgcode.Syntax,
					"SHOW STATEMENT HINTS requires exactly one statement, got %d", len(stmts))
			}
			stmt := stmts[0].AST

			// Some statements are "unwrapped" during execution before looking up and
			// applying hints. Display a notice to the user if this is the case.
			var notice pgnotice.Notice
			switch t := stmt.(type) {
			case *tree.Execute:
				// EXECUTE statements match hints for the underlying prepared statement.
				notice = pgnotice.Newf(
					"EXECUTE statements apply hints from the underlying prepared statement; " +
						"consider using the prepared statement with SHOW STATEMENT HINTS instead.",
				)
			case *tree.CopyTo, *tree.Prepare, *tree.Explain, *tree.ExplainAnalyze:
				// These statements "unwrap" to their inner statements.
				notice = pgnotice.Newf(
					"%s statements apply hints from their inner SQL statement; "+
						"consider using the inner statement with SHOW STATEMENT HINTS instead.",
					t.StatementTag(),
				)
			}
			if notice != nil {
				p.BufferClientNotice(ctx, notice)
			}

			// Compute the fingerprint from the statement.
			fingerprintFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(
				&p.EvalContext().Settings.SV,
			))
			fingerprintStr := tree.FormatStatementHideConstants(stmt, fingerprintFlags)

			// Query system.statement_hints table.
			query := `
SELECT row_id, fingerprint, hint, created_at
FROM system.statement_hints
WHERE fingerprint = $1
ORDER BY created_at DESC, row_id DESC`

			rows, err := p.InternalSQLTxn().QueryBufferedEx(
				ctx,
				"show-statement-hints",
				p.txn,
				sessiondata.NodeUserSessionDataOverride,
				query,
				fingerprintStr,
			)
			if err != nil {
				return nil, err
			}

			v := p.newContainerValuesNode(columns, len(rows))

			for _, row := range rows {
				rowID := row[0]     // INT
				fp := row[1]        // STRING
				hintBytes := row[2] // BYTES
				createdAt := row[3] // TIMESTAMPTZ

				// Decode the hint to determine type and details.
				var hintType tree.Datum = tree.NewDString("unknown")
				var details tree.Datum = tree.DNull

				if hintBytes != tree.DNull {
					hint, err := parseHint(hintBytes)
					if err == nil {
						hintType = tree.NewDString(hint.HintType())
						if n.Options.Details {
							var detailJSON json.JSON
							detailJSON, err = hint.Details()
							if err == nil {
								details = tree.NewDJSON(detailJSON)
							}
						}
					}
					if err != nil {
						// If decode fails, hintType remains "unknown" and details remains
						// NULL. Log the error and continue.
						log.Dev.Warningf(ctx, "failed to decode hint: %v", err)
					}
				}

				var outputRow tree.Datums
				if n.Options.Details {
					outputRow = tree.Datums{rowID, fp, hintType, createdAt, details}
				} else {
					outputRow = tree.Datums{rowID, fp, hintType, createdAt}
				}

				if _, err := v.rows.AddRow(ctx, outputRow); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}

			return v, nil
		},
	}, nil
}

func parseHint(hintBytes tree.Datum) (_ hintpb.StatementHintUnion, retErr error) {
	// Make sure to catch panics in case of malformed hint bytes.
	defer errorutil.MaybeCatchPanic(&retErr, nil /* errCallback */)
	hint, err := hintpb.FromBytes([]byte(tree.MustBeDBytes(hintBytes)))
	if err != nil {
		return hintpb.StatementHintUnion{}, err
	}
	return hint, nil
}
