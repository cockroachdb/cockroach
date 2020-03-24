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
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
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

	diagID, err := execCfg.StmtDiagnosticsRecorder.InsertStatementDiagnostics(
		ctx,
		fingerprint,
		stmtStr,
		traceJSON,
		bundle,
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

// getTraceAndBundle converts the trace to a JSON datum and creates a statement
// bundle. It tries to return as much information as possible even in error
// case.
func getTraceAndBundle(
	trace tracing.Recording, plan *planTop,
) (traceJSON tree.Datum, bundle *bytes.Buffer, _ error) {
	traceJSON, traceStr, err := traceToJSON(trace)
	bundle, bundleErr := buildStatementBundle(plan, trace, traceStr)
	if bundleErr != nil {
		if err == nil {
			err = bundleErr
		} else {
			err = errors.WithMessage(bundleErr, err.Error())
		}
	}
	return traceJSON, bundle, err
}

// traceToJSON converts a trace to a JSON datum suitable for the
// system.statement_diagnostics.trace column. In case of error, the returned
// datum is DNull. Also returns the string representation of the trace.
//
// traceToJSON assumes that the first span in the recording contains all the
// other spans.
func traceToJSON(trace tracing.Recording) (tree.Datum, string, error) {
	root := normalizeSpan(trace[0], trace)
	marshaller := jsonpb.Marshaler{
		Indent: "  ",
	}
	str, err := marshaller.MarshalToString(&root)
	if err != nil {
		return tree.DNull, "", err
	}
	d, err := tree.ParseDJSON(str)
	if err != nil {
		return tree.DNull, "", err
	}
	return d, str, nil
}

func normalizeSpan(s tracing.RecordedSpan, trace tracing.Recording) tracing.NormalizedSpan {
	var n tracing.NormalizedSpan
	n.Operation = s.Operation
	n.StartTime = s.StartTime
	n.Duration = s.Duration
	n.Tags = s.Tags
	n.Logs = s.Logs

	for _, ss := range trace {
		if ss.ParentSpanID != s.SpanID {
			continue
		}
		n.Children = append(n.Children, normalizeSpan(ss, trace))
	}
	return n
}

// buildStatementBundle collects metadata related the planning and execution of
// the statement, generates a bundle, stores it in the
// system.statement_bundle_chunks table and adds an entry in
// system.statement_diagnostics.
//
// Returns the bundle ID, which is the key for the row added in
// statement_diagnostics.
func buildStatementBundle(
	plan *planTop, trace tracing.Recording, traceJSONStr string,
) (*bytes.Buffer, error) {
	if plan == nil {
		return nil, errors.AssertionFailedf("execution terminated early")
	}
	b := makeStmtBundleBuilder(plan)

	b.addStatement()
	b.addOptPlans()
	b.addExecPlan()
	b.addDistSQLDiagrams(trace)
	b.addTrace(traceJSONStr)

	return b.finalize()
}

// stmtBundleBuilder is a helper for building a statement bundle.
type stmtBundleBuilder struct {
	plan *planTop

	z memZipper
}

func makeStmtBundleBuilder(plan *planTop) stmtBundleBuilder {
	b := stmtBundleBuilder{plan: plan}
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

func (b *stmtBundleBuilder) addDistSQLDiagrams(trace tracing.Recording) {
	for i, d := range b.plan.distSQLDiagrams {
		d.AddSpans(trace)
		_, url, err := d.ToURL()

		var contents string
		if err != nil {
			contents = err.Error()
		} else {
			contents = fmt.Sprintf(`<meta http-equiv="Refresh" content="0; url=%s">`, url.String())
		}

		var filename string
		if len(b.plan.distSQLDiagrams) == 1 {
			filename = "distsql.html"
		} else {
			// TODO(radu): it would be great if we could distinguish between
			// subqueries/main query/postqueries here.
			filename = fmt.Sprintf("distsql-%d.html", i+1)
		}
		b.z.AddFile(filename, contents)
	}
}

func (b *stmtBundleBuilder) addTrace(traceJSONStr string) {
	if traceJSONStr != "" {
		b.z.AddFile("trace.json", traceJSONStr)
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

// stmtEnvCollector helps with gathering information about the "environment" in
// which a statement was planned or run: version, relevant session settings,
// schema, table statistics.
type stmtEnvCollector struct {
	ctx context.Context
	ie  *InternalExecutor
}

func makeStmtEnvCollector(ctx context.Context, ie *InternalExecutor) stmtEnvCollector {
	return stmtEnvCollector{ctx: ctx, ie: ie}
}

// environmentQuery is a helper to run a query that returns a single string
// value.
func (c *stmtEnvCollector) query(query string) (string, error) {
	var row tree.Datums
	row, err := c.ie.QueryRowEx(
		c.ctx,
		"stmtEnvCollector",
		nil, /* txn */
		sqlbase.NoSessionDataOverride,
		query,
	)
	if err != nil {
		return "", err
	}

	if len(row) != 1 {
		return "", errors.AssertionFailedf(
			"expected env query %q to return a single column, returned %d",
			query, len(row),
		)
	}

	s, ok := row[0].(*tree.DString)
	if !ok {
		return "", errors.AssertionFailedf(
			"expected env query %q to return a DString, returned %T",
			query, row[0],
		)
	}

	return string(*s), nil
}

var testingOverrideExplainEnvVersion string

// TestingOverrideExplainEnvVersion overrides the version reported by
// EXPLAIN (OPT, ENV). Used for testing.
func TestingOverrideExplainEnvVersion(ver string) func() {
	prev := testingOverrideExplainEnvVersion
	testingOverrideExplainEnvVersion = ver
	return func() { testingOverrideExplainEnvVersion = prev }
}

// PrintVersion appends a row of the form:
//  -- Version: CockroachDB CCL v20.1.0 ...
func (c *stmtEnvCollector) PrintVersion(w io.Writer) error {
	version, err := c.query("SELECT version()")
	if err != nil {
		return err
	}
	if testingOverrideExplainEnvVersion != "" {
		version = testingOverrideExplainEnvVersion
	}
	fmt.Fprintf(w, "-- Version: %s\n", version)
	return err
}

// PrintSettings appends information about session settings that can impact
// planning decisions.
func (c *stmtEnvCollector) PrintSettings(w io.Writer) error {
	relevantSettings := []struct {
		sessionSetting string
		clusterSetting settings.WritableSetting
	}{
		{sessionSetting: "reorder_joins_limit", clusterSetting: ReorderJoinsLimitClusterValue},
		{sessionSetting: "enable_zigzag_join", clusterSetting: zigzagJoinClusterMode},
		{sessionSetting: "optimizer_foreign_keys", clusterSetting: optDrivenFKClusterMode},
	}

	for _, s := range relevantSettings {
		value, err := c.query(fmt.Sprintf("SHOW %s", s.sessionSetting))
		if err != nil {
			return err
		}
		// Get the default value for the cluster setting.
		def := s.clusterSetting.EncodedDefault()
		// Convert true/false to on/off to match what SHOW returns.
		switch def {
		case "true":
			def = "on"
		case "false":
			def = "off"
		}

		if value == def {
			fmt.Fprintf(w, "-- %s has the default value: %s\n", s.sessionSetting, value)
		} else {
			fmt.Fprintf(w, "SET %s = %s;  -- default value: %s\n", s.sessionSetting, value, def)
		}
	}
	return nil
}

func (c *stmtEnvCollector) PrintCreateTable(w io.Writer, tn *tree.TableName) error {
	createStatement, err := c.query(
		fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s]", tn.String()),
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) PrintCreateSequence(w io.Writer, tn *tree.TableName) error {
	createStatement, err := c.query(fmt.Sprintf(
		"SELECT create_statement FROM [SHOW CREATE SEQUENCE %s]", tn.String(),
	))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) PrintCreateView(w io.Writer, tn *tree.TableName) error {
	createStatement, err := c.query(fmt.Sprintf(
		"SELECT create_statement FROM [SHOW CREATE VIEW %s]", tn.String(),
	))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) PrintTableStats(
	w io.Writer, tn *tree.TableName, hideHistograms bool,
) error {
	var maybeRemoveHistoBuckets string
	if hideHistograms {
		maybeRemoveHistoBuckets = " - 'histo_buckets'"
	}

	stats, err := c.query(fmt.Sprintf(
		`SELECT jsonb_pretty(COALESCE(json_agg(stat), '[]'))
		 FROM (
			 SELECT json_array_elements(statistics)%s AS stat
			 FROM [SHOW STATISTICS USING JSON FOR TABLE %s]
		 )`,
		maybeRemoveHistoBuckets, tn.String(),
	))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "ALTER TABLE %s INJECT STATISTICS '%s';\n", tn.String(), stats)
	return nil
}
