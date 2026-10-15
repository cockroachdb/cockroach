// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// This file houses the classifiers that decide whether a statement may carry
// a secret, which the observability surfaces that record statement text,
// placeholder values, or errors consult - the active-query registry, SQL
// event details, statement logs and statistics, and statement bundles - to
// keep secrets out of diagnostics artifacts, while privileged interactive
// readback (e.g. SHOW CLUSTER SETTING) remains unaffected.
//
// Two stages contribute, and each records what it alone can see:
//
//   - Parsing. astMayCarrySecret classifies the AST by statement form, and
//     the result is stored on Statement.MayCarrySecret when the Statement is
//     built.
//   - Planning. isSecretBearingTable recognizes a secret-bearing system table
//     in a plan, something the AST cannot reveal (the internal executor's
//     UPSERT INTO system.settings looks like any other UPSERT). During
//     execution, optbuild reports each table it resolves, for the statement's
//     own plan and for the routine bodies, cascades, and triggers planned as
//     it runs. A memo from the query cache or a prepared statement is not
//     optbuilt again, so reuseMemo reports its tables instead. Both report
//     through planner.NotePlannedTable, and a match is recorded on the
//     instrumentation helper's accumulator. At PREPARE time,
//     recordPrepareSecretFinding inspects the memo and records a match on
//     prep.Metadata.MayCarrySecret for the executions that follow.
//
// A surface that runs before planning, such as active-query registration,
// reads Statement.MayCarrySecret, which covers the parse-time finding and
// whatever an earlier PREPARE stored. A surface that runs after planning reads
// planner.stmtMayCarrySecret, which adds the current execution's findings.

// stmtMayCarrySecret reports whether the statement the planner is executing
// may carry a secret, combining every finding made so far. It is the
// classification for the surfaces that run after planning. Before the
// instrumentation helper is set up, and on paths that never set it up, it
// reduces to p.stmt.MayCarrySecret.
func (p *planner) stmtMayCarrySecret() bool {
	return p.stmt.MayCarrySecret ||
		(p.instrumentation.accum != nil && p.instrumentation.accum.mayCarrySecret)
}

// NotePlannedTable is part of the eval.Planner interface.
func (p *planner) NotePlannedTable(tab cat.Table) {
	if p.instrumentation.accum != nil && isSecretBearingTable(tab) {
		p.instrumentation.accum.mayCarrySecret = true
	}
}

// recordPrepareSecretFinding marks the prepared statement as possibly carrying
// a secret when mem, built while preparing p.stmt, reads or writes a
// secret-bearing system table.
func (p *planner) recordPrepareSecretFinding(mem *memo.Memo) {
	if slices.ContainsFunc(mem.Metadata().AllTables(),
		func(tm opt.TableMeta) bool { return isSecretBearingTable(tm.Table) }) {
		p.stmt.Prepared.MayCarrySecret = true
	}
}

// isSecretBearingTable reports whether tab is a secret-bearing system table. It
// matches by name under the system database rather than by descriptor ID
// because several of these tables have dynamically assigned IDs.
func isSecretBearingTable(tab cat.Table) bool {
	return tab.IsSystemTable() &&
		systemschema.IsSecretBearingTable(catconstants.SystemTableName(tab.Name()))
}

// astMayCarrySecret reports whether ast contains, anywhere in statement
// position, a statement whose form can carry a secret in its raw text, bound
// placeholder values, or errors (see isSecretBearingNode for the full set).
//
// The entire AST is walked because a secret-carrying statement can be nested
// where it plans without executing - a CTE body, a [...] statement source, or
// under a PREPARE / EXPLAIN wrapper - and the enclosing statement's raw text,
// placeholder values, and errors then carry the secret onto the same surfaces
// a top-level form would reach.
func astMayCarrySecret(ast tree.Statement) bool {
	if ast == nil {
		// An empty statement carries no secret.
		return false
	}
	var v secretVisitor
	tree.WalkStmt(&v, ast)
	return v.mayCarrySecret
}

// isSensitiveClusterSettingNode reports whether stmt is a SET CLUSTER SETTING
// (or ALTER VIRTUAL CLUSTER ... SET CLUSTER SETTING) node targeting a
// sensitive setting. It matches only the bare node.
func isSensitiveClusterSettingNode(stmt tree.Statement) bool {
	var name string
	switch t := stmt.(type) {
	case *tree.SetClusterSetting:
		name = t.Name
	case *tree.AlterTenantSetClusterSetting:
		name = t.Name
	default:
		return false
	}
	return settings.IsSensitiveByName(settings.SettingName(strings.ToLower(name)))
}

// hasRolePasswordOption reports whether stmt is a CREATE ROLE or ALTER ROLE
// node with a password option, e.g. CREATE ROLE foo WITH PASSWORD 'secret'.
// It matches only the bare node.
func hasRolePasswordOption(stmt tree.Statement) bool {
	var opts tree.KVOptions
	switch n := stmt.(type) {
	case *tree.CreateRole:
		opts = n.KVOptions
	case *tree.AlterRole:
		opts = n.KVOptions
	default:
		return false
	}
	for _, opt := range opts {
		// Password options are recognized by key suffix, mirroring
		// KVOptions.formatAsRoleOptions.
		if strings.HasSuffix(string(opt.Key), "password") {
			return true
		}
	}
	return false
}

// isSecretBearingNode reports whether the bare stmt node - ignoring any
// nested statements, which the walk visits on their own - has a form that
// can carry a secret.
//
// The bulk-I/O and replication statements below are recognized by type: each
// always (or, for the conservatively-flagged variants, may) reference an
// external URI, KMS URI, connection string, or encryption passphrase that can
// embed credentials, and the redaction that keeps those out of formatted text
// (see FormatURI and PasswordSubstitution in pkg/sql/sem/tree) does not reach
// the raw-text, placeholder, and error surfaces this classifier gates. Matching
// by type rather than by the specific secret-bearing field keeps the set easy to
// audit; over-flagging a secret-free variant of a rare statement only hides its
// constants on those surfaces, which is harmless.
func isSecretBearingNode(stmt tree.Statement) bool {
	switch t := stmt.(type) {
	case *tree.Execute:
		// EXECUTE's argument list may itself be the secret, and its target's
		// secrets are unknowable until the prepared statement is resolved.
		return true

	case *tree.CopyFrom:
		// COPY names an external URI only in the WITH (destination = ...)
		// file-upload form; COPY ... FROM STDIN carries its data out of band and
		// is the common bulk-ingest path, so it is not flagged.
		return t.Options.Destination != nil
	case *tree.CopyTo:
		return t.Options.Destination != nil

	case *tree.AlterTenantReplication:
		// Only the START REPLICATION form names a source connection URI; the
		// COMPLETE / PAUSE / RESUME failover forms do not.
		return t.ReplicationSourceConnUri != nil

	case *tree.Backup, *tree.Restore, *tree.ShowBackup, *tree.AlterBackup,
		*tree.ScheduledBackup, *tree.AlterBackupSchedule,
		*tree.Import, *tree.Export,
		*tree.CreateChangefeed, *tree.AlterChangefeed, *tree.ScheduledChangefeed,
		*tree.CreateExternalConnection,
		*tree.CheckExternalConnection,
		*tree.CreateTenantFromReplication,
		*tree.CreateLogicalReplicationStream:
		return true
	}

	return isSensitiveClusterSettingNode(stmt) || hasRolePasswordOption(stmt)
}

// secretVisitor implements astMayCarrySecret's walk, short-circuiting once a
// secret-bearing node is found. Each node is classified by isSecretBearingNode
// (see it for the recognized forms).
//
// It must be a tree.ExtendedVisitor (rather than a plain tree.Visitor) for
// the walk to reach statements in table-expression position, such as a [...]
// statement source. The walk never modifies the tree.
type secretVisitor struct {
	mayCarrySecret bool
}

var _ tree.ExtendedVisitor = &secretVisitor{}

func (v *secretVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	return !v.mayCarrySecret, expr
}

func (v *secretVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

func (v *secretVisitor) VisitTablePre(expr tree.TableExpr) (recurse bool, newExpr tree.TableExpr) {
	return !v.mayCarrySecret, expr
}

func (v *secretVisitor) VisitTablePost(expr tree.TableExpr) tree.TableExpr { return expr }

func (v *secretVisitor) VisitStatementPre(
	stmt tree.Statement,
) (recurse bool, newStmt tree.Statement) {
	if !v.mayCarrySecret && isSecretBearingNode(stmt) {
		v.mayCarrySecret = true
	}
	if p, ok := stmt.(*tree.Prepare); ok && !v.mayCarrySecret {
		// PREPARE does not implement the statement walk, so descend into its
		// inner statement here.
		tree.WalkStmt(v, p.Statement)
	}
	return !v.mayCarrySecret, stmt
}

func (v *secretVisitor) VisitStatementPost(stmt tree.Statement) tree.Statement { return stmt }
