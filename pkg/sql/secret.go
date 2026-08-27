// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// This file houses the classifier that decides whether a statement may carry
// a secret: a sensitive cluster setting value (see settings.Sensitive), a role
// password, or an external URI, KMS URI, connection string, or encryption
// passphrase (which can embed credentials) referenced by a bulk-I/O or
// replication statement. Observability surfaces that record statement text,
// placeholder values, or errors - the active-query registry, SQL event
// details, statement logs and statistics, and statement bundles - consult it
// to keep such secrets out of diagnostics artifacts, while privileged
// interactive readback (e.g. SHOW CLUSTER SETTING) remains unaffected.

// stmtMayHaveSecret reports whether ast may carry a secret in its raw text,
// bound placeholder values, or errors. That is the case when the statement
// contains, anywhere in statement position, a secret-carrying form (see
// secretVisitor for the full set).
//
// The entire AST is walked because a secret-carrying statement can be nested
// where it plans without executing - a CTE body, a [...] statement source, or
// under a PREPARE / EXPLAIN wrapper - and the enclosing statement's raw text,
// placeholder values, and errors then carry the secret onto the same surfaces
// a top-level form would reach.
func stmtMayHaveSecret(ast tree.Statement) bool {
	if ast == nil {
		// An empty statement carries no secret.
		return false
	}
	var v secretVisitor
	tree.WalkStmt(&v, ast)
	return v.mayHaveSecret
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

// statementNodeMayHaveSecret reports whether the bare stmt node - ignoring any
// nested statements, which the walk visits on their own - may carry a secret.
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
func statementNodeMayHaveSecret(stmt tree.Statement) bool {
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
		*tree.CreateExternalConnection, *tree.AlterExternalConnection,
		*tree.CheckExternalConnection,
		*tree.CreateTenantFromReplication,
		*tree.CreateLogicalReplicationStream:
		return true
	}

	return isSensitiveClusterSettingNode(stmt) || hasRolePasswordOption(stmt)
}

// secretVisitor implements stmtMayHaveSecret's walk, short-circuiting once a
// secret-carrying form is found. Each node is classified by
// statementNodeMayHaveSecret (see it for the recognized forms).
//
// It must be a tree.ExtendedVisitor (rather than a plain tree.Visitor) for
// the walk to reach statements in table-expression position, such as a [...]
// statement source. The walk never modifies the tree.
type secretVisitor struct {
	mayHaveSecret bool
}

var _ tree.ExtendedVisitor = &secretVisitor{}

func (v *secretVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	return !v.mayHaveSecret, expr
}

func (v *secretVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

func (v *secretVisitor) VisitTablePre(expr tree.TableExpr) (recurse bool, newExpr tree.TableExpr) {
	return !v.mayHaveSecret, expr
}

func (v *secretVisitor) VisitTablePost(expr tree.TableExpr) tree.TableExpr { return expr }

func (v *secretVisitor) VisitStatementPre(
	stmt tree.Statement,
) (recurse bool, newStmt tree.Statement) {
	if !v.mayHaveSecret && statementNodeMayHaveSecret(stmt) {
		v.mayHaveSecret = true
	}
	if p, ok := stmt.(*tree.Prepare); ok && !v.mayHaveSecret {
		// PREPARE does not implement the statement walk, so descend into its
		// inner statement here.
		tree.WalkStmt(v, p.Statement)
	}
	return !v.mayHaveSecret, stmt
}

func (v *secretVisitor) VisitStatementPost(stmt tree.Statement) tree.Statement { return stmt }
