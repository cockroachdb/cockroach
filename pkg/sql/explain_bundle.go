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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// setExplainBundleResult creates the diagnostics and returns the bundle
// information for an EXPLAIN BUNDLE statement.
//
// Returns an error if information rows couldn't be added to the result.
func setExplainBundleResult(
	ctx context.Context,
	res RestrictedCommandResult,
	ast tree.Statement,
	trace tracing.Recording,
	plan *planTop,
	execCfg *ExecutorConfig,
) error {
	res.ResetStmtType(&tree.ExplainBundle{})
	res.SetColumns(ctx, sqlbase.ExplainBundleColumns)

	traceJSON, bundle, err := getTraceAndBundle(trace, plan)
	if err != nil {
		res.SetError(err)
		return nil
	}

	fingerprint := tree.AsStringWithFlags(ast, tree.FmtHideConstants)
	stmtStr := tree.AsString(ast)

	diagID, err := insertStatementDiagnostics(
		ctx,
		execCfg.DB,
		execCfg.InternalExecutor,
		0, /* requestID */
		fingerprint,
		stmtStr,
		traceJSON,
		bundle,
		nil, /* collectionErr */
	)
	if err != nil {
		res.SetError(err)
		return nil
	}

	url := fmt.Sprintf("  %s/_admin/v1/stmtbundle/%d", execCfg.AdminURL(), diagID)
	text := []string{
		"Download the bundle from:",
		url,
		"or from the Admin UI (Advanced Debug -> Statement Diagnostics).",
	}

	if err := res.Err(); err != nil {
		// Add the bundle information as a detail to the query error.
		//
		// TODO(radu): if the statement gets auto-retried, we will generate a
		// bundle for each iteration. If the statement eventually succeeds we
		// will have a link to the last iteration's bundle. It's not clear what
		// the ideal behavior is here; if we keep all bundles we should try to
		// list them all in the final message.
		res.SetError(errors.WithDetail(err, strings.Join(text, "\n")))
		return nil
	}

	for _, line := range text {
		if err := res.AddRow(ctx, tree.Datums{tree.NewDString(line)}); err != nil {
			return err
		}
	}
	return nil
}

// buildStatementBundle collects metadata related the planning and execution of
// the statement, generates a bundle, stores it in the
// system.statement_bundle_chunks table and adds an entry in
// system.statement_diagnostics.
//
// Returns the bundle ID, which is the key for the row added in
// statement_diagnostics.
func buildStatementBundle(plan *planTop, trace string) (*bytes.Buffer, error) {
	if plan == nil {
		return nil, errors.AssertionFailedf("execution terminated early")
	}
	b := makeStmtBundleBuilder(plan, trace)

	b.addStatement()
	b.addOptPlans()
	b.addExecPlan()
	b.addTrace()

	return b.finalize()
}

// stmtBundleBuilder is a helper for building a statement bundle.
type stmtBundleBuilder struct {
	plan *planTop

	// trace is the recorded trace (formatted as JSON).
	trace string

	z memZipper
}

func makeStmtBundleBuilder(plan *planTop, trace string) stmtBundleBuilder {
	b := stmtBundleBuilder{plan: plan, trace: trace}
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

	b.z.AddFile("statement.txt", cfg.Pretty(b.plan.stmt.AST))
}

// addOptPlans adds the EXPLAIN (OPT) variants as files opt.txt, opt-v.txt,
// opt-vv.txt.
func (b *stmtBundleBuilder) addOptPlans() {
	if b.plan.mem == nil {
		// No optimizer plans; an error must have occurred during planning.
		return
	}

	b.z.AddFile("opt.txt", b.plan.formatOptPlan(memo.ExprFmtHideAll))
	b.z.AddFile("opt-v.txt", b.plan.formatOptPlan(
		memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes,
	))
	b.z.AddFile("opt-vv.txt", b.plan.formatOptPlan(memo.ExprFmtHideQualifications))
}

// addExecPlan adds the EXPLAIN (VERBOSE) plan as file plan.txt.
func (b *stmtBundleBuilder) addExecPlan() {
	if plan := b.plan.instrumentation.planString; plan != "" {
		b.z.AddFile("plan.txt", plan)
	}
}

func (b *stmtBundleBuilder) addTrace() {
	if b.trace != "" {
		b.z.AddFile("trace.json", b.trace)
	}
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
