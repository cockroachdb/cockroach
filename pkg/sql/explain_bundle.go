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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

// setExplainBundleResult creates the diagnostics and returns the bundle
// information for an EXPLAIN ANALYZE (DEBUG) statement.
//
// Returns an error if information rows couldn't be added to the result.
func setExplainBundleResult(
	ctx context.Context,
	res RestrictedCommandResult,
	ast tree.Statement,
	trace tracing.Recording,
	plan *planTop,
	ie *InternalExecutor,
	execCfg *ExecutorConfig,
) error {
	res.ResetStmtType(&tree.ExplainAnalyzeDebug{})
	res.SetColumns(ctx, sqlbase.ExplainAnalyzeDebugColumns)

	var text []string
	func() {
		bundle, err := buildStatementBundle(ctx, execCfg.DB, ie, plan, trace)
		if err != nil {
			// TODO(radu): we cannot simply set an error on the result here without
			// changing the executor logic (e.g. an implicit transaction could have
			// committed already). Just show the error in the result.
			text = []string{fmt.Sprintf("Error generating bundle: %v", err)}
			return
		}

		fingerprint := tree.AsStringWithFlags(ast, tree.FmtHideConstants)
		stmtStr := tree.AsString(ast)

		diagID, err := execCfg.StmtDiagnosticsRecorder.InsertStatementDiagnostics(
			ctx,
			fingerprint,
			stmtStr,
			bundle.trace,
			bundle.zip,
		)
		if err != nil {
			text = []string{fmt.Sprintf("Error recording bundle: %v", err)}
			return
		}

		text = []string{
			"Statement diagnostics bundle generated. Download from the Admin UI (Advanced",
			"Debug -> Statement Diagnostics History) or use the direct link below.",
			fmt.Sprintf("Admin UI: %s", execCfg.AdminURL()),
			fmt.Sprintf("Direct link: %s/_admin/v1/stmtbundle/%d", execCfg.AdminURL(), diagID),
		}
	}()

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

// traceToJSON converts a trace to a JSON datum suitable for the
// system.statement_diagnostics.trace column. In case of error, the returned
// datum is DNull. Also returns the string representation of the trace.
//
// traceToJSON assumes that the first span in the recording contains all the
// other spans.
func traceToJSON(trace tracing.Recording) (tree.Datum, string, error) {
	root := normalizeSpan(trace[0], trace)
	marshaller := jsonpb.Marshaler{
		Indent: "\t",
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

// diagnosticsBundle contains diagnostics information collected for a statement.
type diagnosticsBundle struct {
	zip   []byte
	trace tree.Datum
}

// buildStatementBundle collects metadata related the planning and execution of
// the statement. It generates a bundle for storage in
// system.statement_diagnostics.
func buildStatementBundle(
	ctx context.Context, db *kv.DB, ie *InternalExecutor, plan *planTop, trace tracing.Recording,
) (diagnosticsBundle, error) {
	if plan == nil {
		return diagnosticsBundle{}, errors.AssertionFailedf("execution terminated early")
	}
	b := makeStmtBundleBuilder(db, ie, plan, trace)

	b.addStatement()
	b.addOptPlans()
	b.addExecPlan()
	b.addDistSQLDiagrams()
	traceJSON := b.addTrace()
	b.addEnv(ctx)

	buf, err := b.finalize()
	if err != nil {
		return diagnosticsBundle{}, err
	}
	return diagnosticsBundle{trace: traceJSON, zip: buf.Bytes()}, nil
}

// stmtBundleBuilder is a helper for building a statement bundle.
type stmtBundleBuilder struct {
	db *kv.DB
	ie *InternalExecutor

	plan  *planTop
	trace tracing.Recording

	z memZipper
}

func makeStmtBundleBuilder(
	db *kv.DB, ie *InternalExecutor, plan *planTop, trace tracing.Recording,
) stmtBundleBuilder {
	b := stmtBundleBuilder{db: db, ie: ie, plan: plan, trace: trace}
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

func (b *stmtBundleBuilder) addDistSQLDiagrams() {
	for i, d := range b.plan.distSQLDiagrams {
		d.AddSpans(b.trace)
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

// addTrace adds two files to the bundle: one is a json representation of the
// trace, the other one is a human-readable representation.
func (b *stmtBundleBuilder) addTrace() tree.Datum {
	traceJSON, traceJSONStr, err := traceToJSON(b.trace)
	if err != nil {
		b.z.AddFile("trace.json", err.Error())
	} else {
		b.z.AddFile("trace.json", traceJSONStr)
	}

	cfg := tree.DefaultPrettyCfg()
	cfg.UseTabs = false
	cfg.LineWidth = 100
	cfg.TabWidth = 2
	cfg.Simplify = true
	cfg.Align = tree.PrettyNoAlign
	cfg.JSONFmt = true
	stmt := cfg.Pretty(b.plan.stmt.AST)

	// The JSON is not very human-readable, so we include another format too.
	b.z.AddFile("trace.txt", fmt.Sprintf("%s\n\n\n\n%s", stmt, b.trace.String()))

	// Note that we're going to include the non-anonymized statement in the trace.
	// But then again, nothing in the trace is anonymized.
	jaegerJSON, err := b.trace.ToJaegerJSON(stmt)
	if err != nil {
		b.z.AddFile("trace-jaeger.txt", err.Error())
	} else {
		b.z.AddFile("trace-jaeger.json", jaegerJSON)
	}

	return traceJSON
}

func (b *stmtBundleBuilder) addEnv(ctx context.Context) {
	c := makeStmtEnvCollector(ctx, b.ie)

	var buf bytes.Buffer
	if err := c.PrintVersion(&buf); err != nil {
		fmt.Fprintf(&buf, "-- error getting version: %v\n", err)
	}
	fmt.Fprintf(&buf, "\n")

	// Show the values of any non-default session variables that can impact
	// planning decisions.
	if err := c.PrintSettings(&buf); err != nil {
		fmt.Fprintf(&buf, "-- error getting settings: %v\n", err)
	}
	b.z.AddFile("env.sql", buf.String())

	mem := b.plan.mem
	if mem == nil {
		// No optimizer plans; an error must have occurred during planning.
		return
	}
	buf.Reset()

	var tables, sequences, views []tree.TableName
	err := b.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		tables, sequences, views, err = mem.Metadata().AllDataSourceNames(
			func(ds cat.DataSource) (cat.DataSourceName, error) {
				return b.plan.catalog.fullyQualifiedNameWithTxn(ctx, ds, txn)
			},
		)
		return err
	})
	if err != nil {
		b.z.AddFile("schema.sql", fmt.Sprintf("-- error getting data source names: %v\n", err))
		return
	}

	if len(tables) == 0 && len(sequences) == 0 && len(views) == 0 {
		return
	}

	first := true
	blankLine := func() {
		if !first {
			buf.WriteByte('\n')
		}
		first = false
	}
	for i := range sequences {
		blankLine()
		if err := c.PrintCreateSequence(&buf, &sequences[i]); err != nil {
			fmt.Fprintf(&buf, "-- error getting schema for sequence %s: %v\n", sequences[i].String(), err)
		}
	}
	for i := range tables {
		blankLine()
		if err := c.PrintCreateTable(&buf, &tables[i]); err != nil {
			fmt.Fprintf(&buf, "-- error getting schema for table %s: %v\n", tables[i].String(), err)
		}
	}
	for i := range views {
		blankLine()
		if err := c.PrintCreateView(&buf, &views[i]); err != nil {
			fmt.Fprintf(&buf, "-- error getting schema for view %s: %v\n", views[i].String(), err)
		}
	}
	b.z.AddFile("schema.sql", buf.String())
	for i := range tables {
		buf.Reset()
		if err := c.PrintTableStats(&buf, &tables[i], false /* hideHistograms */); err != nil {
			fmt.Fprintf(&buf, "-- error getting statistics for table %s: %v\n", tables[i].String(), err)
		}
		b.z.AddFile(fmt.Sprintf("stats-%s.sql", tables[i].String()), buf.String())
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
		{sessionSetting: "optimizer_use_histograms", clusterSetting: optUseHistogramsClusterMode},
		{sessionSetting: "optimizer_use_multicol_stats", clusterSetting: optUseMultiColStatsClusterMode},
		// TODO(mgartner): remove this once partial indexes are fully supported.
		{sessionSetting: "experimental_partial_indexes", clusterSetting: partialIndexClusterMode},
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

	stats = strings.Replace(stats, "'", "''", -1)
	fmt.Fprintf(w, "ALTER TABLE %s INJECT STATISTICS '%s';\n", tn.String(), stats)
	return nil
}
