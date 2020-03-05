// Copyright 2020 The Cockroach Authors.
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
	"archive/zip"
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// generateExplainBundle creates the bundle and returns the result for an
// EXPLAIN BUNDLE statement. The result is text containing a URL for the bundle.
func (p *planner) generateExplainBundle(ctx context.Context, res RestrictedCommandResult) error {
	res.ResetStmtType(&tree.ExplainBundle{})
	res.SetColumns(ctx, sqlbase.ExplainBundleColumns)

	id, err := buildStatementBundle(ctx, p)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/_admin/v1/stmtbundle/%d", p.ExecCfg().AdminURL(), id)
	text := fmt.Sprintf(""+
		"Download the bundle from:\n"+
		"  %s\n"+
		"or from the Admin UI (Advanced Debug -> Statement Diagnostics).",
		url,
	)
	return res.AddRow(ctx, tree.Datums{tree.NewDString(text)})
}

// buildStatementBundle collects metadata about the last statement ran in the
// planner, generates a bundle, stores it in the system.statement_bundle_chunks
// table and adds an entry in system.statement_diagnostics.
//
// Returns the bundle ID, which is the key for the row added in
// statement_diagnostics.
func buildStatementBundle(ctx context.Context, p *planner) (bundleID int64, _ error) {
	b := makeStmtBundleBuilder(p)

	b.addStatement()
	b.addOptPlans()
	b.addExecPlan()

	buf, err := b.finalize()
	if err != nil {
		return 0, err
	}

	db, ie := p.execCfg.DB, p.execCfg.InternalExecutor
	fingerprint := tree.AsStringWithFlags(p.stmt.AST, tree.FmtHideConstants)
	statement := tree.AsString(p.stmt.AST)
	description := "query support bundle XXX more info here"

	err = db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		// Insert the bundle into system.statement_bundle_chunks.
		// TODO(radu): split in chunks.
		row, err := ie.QueryRowEx(
			ctx, "statement-bundle-chunks-insert", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			"INSERT INTO system.statement_bundle_chunks(description, data) VALUES ($1, $2) RETURNING id",
			description,
			tree.NewDBytes(tree.DBytes(buf.String())),
		)
		if err != nil {
			return err
		}
		chunkID := row[0].(*tree.DInt)
		chunks := tree.NewDArray(types.Int)
		if err := chunks.Append(chunkID); err != nil {
			return err
		}

		row, err = ie.QueryRowEx(
			ctx, "statement-bundle-info-insert", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`INSERT INTO
			  system.statement_diagnostics(statement_fingerprint, statement, collected_at, bundle_chunks)
			  VALUES ($1, $2, $3, $4)
				RETURNING id
				`,
			fingerprint, statement, timeutil.Now(), chunks,
		)
		if err != nil {
			return err
		}
		bundleID = int64(*row[0].(*tree.DInt))
		return err
	})
	return bundleID, err
}

// stmtBundleBuilder is a helper for building a statement bundle.
type stmtBundleBuilder struct {
	p *planner
	z memZipper
}

func makeStmtBundleBuilder(p *planner) stmtBundleBuilder {
	b := stmtBundleBuilder{p: p}
	b.z.Init()
	return b
}

// addStatement adds the pretty-printed statement as file statement.txt.
func (b *stmtBundleBuilder) addStatement() {
	cfg := tree.DefaultPrettyCfg()
	cfg.UseTabs = false
	cfg.LineWidth = 100
	cfg.TabWidth = 2
	cfg.Simplify = true
	cfg.Align = tree.PrettyNoAlign
	cfg.JSONFmt = true

	b.z.AddFile("statement.txt", cfg.Pretty(b.p.stmt.AST))
}

// addOptPlans adds the EXPLAIN (OPT) variants as files opt.txt, opt-v.txt,
// opt-vv.txt.
func (b *stmtBundleBuilder) addOptPlans() {
	b.z.AddFile("opt.txt", b.p.FormatOptPlan(memo.ExprFmtHideAll))
	b.z.AddFile("opt-v.txt", b.p.FormatOptPlan(
		memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes,
	))
	b.z.AddFile("opt-vv.txt", b.p.FormatOptPlan(memo.ExprFmtHideQualifications))
}

// addExecPlan adds the EXPLAIN (VERBOSE) plan as file plan.txt.
func (b *stmtBundleBuilder) addExecPlan() {
	b.z.AddFile("plan.txt", b.p.curPlan.instrumentation.planString)
}

// finalize generates the zipped bundle and returns it as a buffer.
func (b *stmtBundleBuilder) finalize() (*bytes.Buffer, error) {
	return b.z.Finalize()
}

// memZipper builds a zip file into an in-memory buffer.
type memZipper struct {
	buf *bytes.Buffer
	z   *zip.Writer
	err error
}

func (z *memZipper) Init() {
	z.buf = &bytes.Buffer{}
	z.z = zip.NewWriter(z.buf)
}

func (z *memZipper) AddFile(name string, contents string) {
	if z.err != nil {
		return
	}
	w, err := z.z.CreateHeader(&zip.FileHeader{
		Name:     name,
		Method:   zip.Deflate,
		Modified: timeutil.Now(),
	})
	if err != nil {
		z.err = err
		return
	}
	_, z.err = w.Write([]byte(contents))
}

func (z *memZipper) Finalize() (*bytes.Buffer, error) {
	if z.err != nil {
		return nil, z.err
	}
	if err := z.z.Close(); err != nil {
		return nil, err
	}
	buf := z.buf
	*z = memZipper{}
	return buf, nil
}
