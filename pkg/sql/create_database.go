// Copyright 2017 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type createDatabaseNode struct {
	n *tree.CreateDatabase
}

// CreateDatabase creates a database.
// Privileges: superuser.
//   Notes: postgres requires superuser or "CREATEDB".
//          mysql uses the mysqladmin command.
func (p *planner) CreateDatabase(ctx context.Context, n *tree.CreateDatabase) (planNode, error) {
	if n.Name == "" {
		return nil, errEmptyDatabaseName
	}

	if tmpl := n.Template; tmpl != "" {
		// See https://www.postgresql.org/docs/current/static/manage-ag-templatedbs.html
		if !strings.EqualFold(tmpl, "template0") {
			return nil, unimplemented.NewWithIssuef(10151,
				"unsupported template: %s", tmpl)
		}
	}

	if enc := n.Encoding; enc != "" {
		// We only support UTF8 (and aliases for UTF8).
		if !(strings.EqualFold(enc, "UTF8") ||
			strings.EqualFold(enc, "UTF-8") ||
			strings.EqualFold(enc, "UNICODE")) {
			return nil, unimplemented.NewWithIssueDetailf(35882, "create.db.encoding",
				"unsupported encoding: %s", enc)
		}
	}

	if col := n.Collate; col != "" {
		// We only support C and C.UTF-8.
		if col != "C" && col != "C.UTF-8" {
			return nil, unimplemented.NewWithIssueDetailf(16618, "create.db.collation",
				"unsupported collation: %s", col)
		}
	}

	if ctype := n.CType; ctype != "" {
		// We only support C and C.UTF-8.
		if ctype != "C" && ctype != "C.UTF-8" {
			return nil, unimplemented.NewWithIssueDetailf(35882, "create.db.classification",
				"unsupported character classification: %s", ctype)
		}
	}

	if err := p.RequireAdminRole(ctx, "CREATE DATABASE"); err != nil {
		return nil, err
	}

	return &createDatabaseNode{n: n}, nil
}

func (n *createDatabaseNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("database"))

	desc, created, err := params.p.createDatabase(
		params.ctx, n.n, tree.AsStringWithFQNames(n.n, params.Ann()))
	if err != nil {
		return err
	}
	if created {
		// Log Create Database event. This is an auditable log event and is
		// recorded in the same transaction as the table descriptor update.
		if err := MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
			params.ctx,
			params.p.txn,
			EventLogCreateDatabase,
			int32(desc.GetID()),
			int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
			struct {
				DatabaseName string
				Statement    string
				User         string
			}{n.n.Name.String(), n.n.String(), params.SessionData().User},
		); err != nil {
			return err
		}
		params.extendedEvalCtx.Descs.AddUncommittedDatabase(
			desc.GetName(), desc.GetID(), descs.DBCreated)
	}
	return nil
}

func (*createDatabaseNode) Next(runParams) (bool, error) { return false, nil }
func (*createDatabaseNode) Values() tree.Datums          { return tree.Datums{} }
func (*createDatabaseNode) Close(context.Context)        {}
