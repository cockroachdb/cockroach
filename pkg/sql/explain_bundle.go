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
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// setExplainBundleResult sets the result of an EXPLAIN ANALYZE (DEBUG)
// statement.
//
// Note: bundle.insert() must have been called.
//
// Returns an error if information rows couldn't be added to the result.
func setExplainBundleResult(
	ctx context.Context,
	res RestrictedCommandResult,
	bundle diagnosticsBundle,
	execCfg *ExecutorConfig,
) error {
	res.ResetStmtType(&tree.ExplainAnalyze{})
	res.SetColumns(ctx, colinfo.ExplainPlanColumns)

	var text []string
	if bundle.collectionErr != nil {
		// TODO(radu): we cannot simply set an error on the result here without
		// changing the executor logic (e.g. an implicit transaction could have
		// committed already). Just show the error in the result.
		text = []string{fmt.Sprintf("Error generating bundle: %v", bundle.collectionErr)}
	} else if execCfg.Codec.ForSystemTenant() {
		text = []string{
			"Statement diagnostics bundle generated. Download from the Admin UI (Advanced",
			"Debug -> Statement Diagnostics History), via the direct link below, or using",
			"the SQL shell or command line.",
			fmt.Sprintf("Admin UI: %s", execCfg.AdminURL()),
			fmt.Sprintf("Direct link: %s/_admin/v1/stmtbundle/%d", execCfg.AdminURL(), bundle.diagID),
			fmt.Sprintf("SQL shell: \\statement-diag download %d", bundle.diagID),
			fmt.Sprintf("Command line: cockroach statement-diag download %d", bundle.diagID),
		}
	} else {
		// Non-system tenants can't directly access the AdminUI.
		// TODO(radu): update the message when Serverless provides a way to download
		// the bundle (preferably using a more general mechanism so as not to bake
		// in Serverless specifics).
		text = []string{
			"Statement diagnostics bundle generated. Download using the SQL shell or command",
			"line.",
			fmt.Sprintf("SQL shell: \\statement-diag download %d", bundle.diagID),
			fmt.Sprintf("Command line: cockroach statement-diag download %d", bundle.diagID),
		}
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

// diagnosticsBundle contains diagnostics information collected for a statement.
type diagnosticsBundle struct {
	// Zip file binary data.
	zip []byte

	// Stores any error in the collection, building, or insertion of the bundle.
	collectionErr error

	// diagID is the diagnostics instance ID, populated by insert().
	diagID stmtdiagnostics.CollectedInstanceID
}

// buildStatementBundle collects metadata related to the planning and execution
// of the statement. It generates a bundle for storage in
// system.statement_diagnostics.
func buildStatementBundle(
	ctx context.Context,
	db *kv.DB,
	ie *InternalExecutor,
	plan *planTop,
	planString string,
	trace tracing.Recording,
	placeholders *tree.PlaceholderInfo,
) diagnosticsBundle {
	if plan == nil {
		return diagnosticsBundle{collectionErr: errors.AssertionFailedf("execution terminated early")}
	}
	b := makeStmtBundleBuilder(db, ie, plan, trace, placeholders)

	b.addStatement()
	b.addOptPlans()
	b.addExecPlan(planString)
	b.addDistSQLDiagrams()
	b.addExplainVec()
	b.addTrace()
	b.addEnv(ctx)

	buf, err := b.finalize()
	if err != nil {
		return diagnosticsBundle{collectionErr: err}
	}
	return diagnosticsBundle{zip: buf.Bytes()}
}

// insert the bundle in statement diagnostics. Sets bundle.diagID and (in error
// cases) bundle.collectionErr.
//
// diagRequestID should be the ID returned by ShouldCollectDiagnostics, or zero
// if diagnostics were triggered by EXPLAIN ANALYZE (DEBUG).
func (bundle *diagnosticsBundle) insert(
	ctx context.Context,
	fingerprint string,
	ast tree.Statement,
	stmtDiagRecorder *stmtdiagnostics.Registry,
	diagRequestID stmtdiagnostics.RequestID,
) {
	var err error
	bundle.diagID, err = stmtDiagRecorder.InsertStatementDiagnostics(
		ctx,
		diagRequestID,
		fingerprint,
		tree.AsString(ast),
		bundle.zip,
		bundle.collectionErr,
	)
	if err != nil {
		log.Warningf(ctx, "failed to report statement diagnostics: %s", err)
		if bundle.collectionErr != nil {
			bundle.collectionErr = err
		}
	}
}

// stmtBundleBuilder is a helper for building a statement bundle.
type stmtBundleBuilder struct {
	db *kv.DB
	ie *InternalExecutor

	plan         *planTop
	trace        tracing.Recording
	placeholders *tree.PlaceholderInfo

	z memzipper.Zipper
}

func makeStmtBundleBuilder(
	db *kv.DB,
	ie *InternalExecutor,
	plan *planTop,
	trace tracing.Recording,
	placeholders *tree.PlaceholderInfo,
) stmtBundleBuilder {
	b := stmtBundleBuilder{db: db, ie: ie, plan: plan, trace: trace, placeholders: placeholders}
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
	var output string
	// If we hit an early error, stmt or stmt.AST might not be initialized yet.
	switch {
	case b.plan.stmt == nil:
		output = "-- No Statement."
	case b.plan.stmt.AST == nil:
		output = "-- No AST."
	default:
		output = cfg.Pretty(b.plan.stmt.AST)
	}

	if b.placeholders != nil && len(b.placeholders.Values) != 0 {
		var buf bytes.Buffer
		buf.WriteString(output)
		buf.WriteString("\n\n-- Arguments:\n")
		for i, v := range b.placeholders.Values {
			fmt.Fprintf(&buf, "--  %s: %v\n", tree.PlaceholderIdx(i), v)
		}
		output = buf.String()
	}

	b.z.AddFile("statement.sql", output)
}

// addOptPlans adds the EXPLAIN (OPT) variants as files opt.txt, opt-v.txt,
// opt-vv.txt.
func (b *stmtBundleBuilder) addOptPlans() {
	if b.plan.mem == nil || b.plan.mem.RootExpr() == nil {
		// No optimizer plans; an error must have occurred during planning.
		return
	}

	formatOptPlan := func(flags memo.ExprFmtFlags) string {
		f := memo.MakeExprFmtCtx(flags, b.plan.mem, b.plan.catalog)
		f.FormatExpr(b.plan.mem.RootExpr())
		return f.Buffer.String()
	}

	b.z.AddFile("opt.txt", formatOptPlan(memo.ExprFmtHideAll))
	b.z.AddFile("opt-v.txt", formatOptPlan(
		memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes,
	))
	b.z.AddFile("opt-vv.txt", formatOptPlan(memo.ExprFmtHideQualifications))
}

// addExecPlan adds the EXPLAIN (VERBOSE) plan as file plan.txt.
func (b *stmtBundleBuilder) addExecPlan(plan string) {
	if plan != "" {
		b.z.AddFile("plan.txt", plan)
	}
}

func (b *stmtBundleBuilder) addDistSQLDiagrams() {
	for i, d := range b.plan.distSQLFlowInfos {
		d.diagram.AddSpans(b.trace)
		_, url, err := d.diagram.ToURL()

		var contents string
		if err != nil {
			contents = err.Error()
		} else {
			contents = fmt.Sprintf(`<meta http-equiv="Refresh" content="0; url=%s">`, url.String())
		}

		var filename string
		if len(b.plan.distSQLFlowInfos) == 1 {
			filename = "distsql.html"
		} else {
			filename = fmt.Sprintf("distsql-%d-%s.html", i+1, d.typ)
		}
		b.z.AddFile(filename, contents)
	}
}

func (b *stmtBundleBuilder) addExplainVec() {
	for i, d := range b.plan.distSQLFlowInfos {
		if len(d.explainVec) > 0 || len(d.explainVecVerbose) > 0 {
			extra := ""
			if len(b.plan.distSQLFlowInfos) > 1 {
				extra = fmt.Sprintf("-%d-%s", i+1, d.typ)
			}
			if len(d.explainVec) > 0 {
				b.z.AddFile(fmt.Sprintf("vec%s.txt", extra), strings.Join(d.explainVec, "\n"))
			}
			if len(d.explainVecVerbose) > 0 {
				b.z.AddFile(fmt.Sprintf("vec%s-v.txt", extra), strings.Join(d.explainVecVerbose, "\n"))
			}
		}
	}
}

// addTrace adds three files to the bundle: two are a json representation of the
// trace (the default and the jaeger formats), the third one is a human-readable
// representation.
func (b *stmtBundleBuilder) addTrace() {
	traceJSONStr, err := tracing.TraceToJSON(b.trace)
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
	comment := fmt.Sprintf(`This is a trace for SQL statement: %s
This trace can be imported into Jaeger for visualization. From the Jaeger Search screen, select the JSON File.
Jaeger can be started using docker with: docker run -d --name jaeger -p 16686:16686 jaegertracing/all-in-one:1.17
The UI can then be accessed at http://localhost:16686/search`, stmt)
	jaegerJSON, err := b.trace.ToJaegerJSON(stmt, comment, "")
	if err != nil {
		b.z.AddFile("trace-jaeger.txt", err.Error())
	} else {
		b.z.AddFile("trace-jaeger.json", jaegerJSON)
	}
}

func (b *stmtBundleBuilder) addEnv(ctx context.Context) {
	c := makeStmtEnvCollector(ctx, b.ie)

	var buf bytes.Buffer
	if err := c.PrintVersion(&buf); err != nil {
		fmt.Fprintf(&buf, "-- error getting version: %v\n", err)
	}
	fmt.Fprintf(&buf, "\n")

	// Show the values of session variables that can impact planning decisions.
	if err := c.PrintSessionSettings(&buf); err != nil {
		fmt.Fprintf(&buf, "-- error getting session settings: %v\n", err)
	}

	fmt.Fprintf(&buf, "\n")

	if err := c.PrintClusterSettings(&buf); err != nil {
		fmt.Fprintf(&buf, "-- error getting cluster settings: %v\n", err)
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
	row, err := c.ie.QueryRowEx(
		c.ctx,
		"stmtEnvCollector",
		nil, /* txn */
		sessiondata.NoSessionDataOverride,
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

// PrintSessionSettings appends information about session settings that can
// impact planning decisions.
func (c *stmtEnvCollector) PrintSessionSettings(w io.Writer) error {
	// Cluster setting encoded default value to session setting value conversion
	// functions.
	boolToOnOff := func(boolStr string) string {
		switch boolStr {
		case "true":
			return "on"
		case "false":
			return "off"
		}
		return boolStr
	}

	distsqlConv := func(enumVal string) string {
		n, err := strconv.ParseInt(enumVal, 10, 32)
		if err != nil {
			return enumVal
		}
		return sessiondatapb.DistSQLExecMode(n).String()
	}

	vectorizeConv := func(enumVal string) string {
		n, err := strconv.ParseInt(enumVal, 10, 32)
		if err != nil {
			return enumVal
		}
		return sessiondatapb.VectorizeExecMode(n).String()
	}

	// TODO(rytaft): Keeping this list up to date is a challenge. Consider just
	// printing all session settings.
	relevantSettings := []struct {
		sessionSetting string
		clusterSetting settings.NonMaskedSetting
		convFunc       func(string) string
	}{
		{sessionSetting: "reorder_joins_limit", clusterSetting: ReorderJoinsLimitClusterValue},
		{sessionSetting: "enable_zigzag_join", clusterSetting: zigzagJoinClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "optimizer_use_histograms", clusterSetting: optUseHistogramsClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "optimizer_use_multicol_stats", clusterSetting: optUseMultiColStatsClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "locality_optimized_partitioned_index_scan", clusterSetting: localityOptimizedSearchMode, convFunc: boolToOnOff},
		{sessionSetting: "propagate_input_ordering", clusterSetting: propagateInputOrdering, convFunc: boolToOnOff},
		{sessionSetting: "prefer_lookup_joins_for_fks", clusterSetting: preferLookupJoinsForFKs, convFunc: boolToOnOff},
		{sessionSetting: "intervalstyle_enabled", clusterSetting: intervalStyleEnabled, convFunc: boolToOnOff},
		{sessionSetting: "datestyle_enabled", clusterSetting: dateStyleEnabled, convFunc: boolToOnOff},
		{sessionSetting: "disallow_full_table_scans", clusterSetting: disallowFullTableScans, convFunc: boolToOnOff},
		{sessionSetting: "large_full_scan_rows", clusterSetting: largeFullScanRows},
		{sessionSetting: "cost_scans_with_default_col_size", clusterSetting: costScansWithDefaultColSize, convFunc: boolToOnOff},
		{sessionSetting: "default_transaction_quality_of_service"},
		{sessionSetting: "distsql", clusterSetting: DistSQLClusterExecMode, convFunc: distsqlConv},
		{sessionSetting: "vectorize", clusterSetting: VectorizeClusterMode, convFunc: vectorizeConv},
	}

	for _, s := range relevantSettings {
		value, err := c.query(fmt.Sprintf("SHOW %s", s.sessionSetting))
		if err != nil {
			return err
		}
		// Get the default value for the cluster setting.
		var def string
		if s.clusterSetting == nil {
			// Special handling for default_transaction_quality_of_service since it
			// has no cluster setting.
			def = sessiondatapb.Normal.String()
		} else {
			def = s.clusterSetting.EncodedDefault()
		}
		if s.convFunc != nil {
			// If necessary, convert the encoded cluster setting to a session setting
			// value (e.g.  "true"->"on"), depending on the setting.
			def = s.convFunc(def)
		}

		if value == def {
			fmt.Fprintf(w, "-- %s has the default value: %s\n", s.sessionSetting, value)
		} else {
			fmt.Fprintf(w, "SET %s = %s;  -- default value: %s\n", s.sessionSetting, value, def)
		}
	}
	return nil
}

func (c *stmtEnvCollector) PrintClusterSettings(w io.Writer) error {
	rows, err := c.ie.QueryBufferedEx(
		c.ctx,
		"stmtEnvCollector",
		nil, /* txn */
		sessiondata.NoSessionDataOverride,
		"SELECT variable, value, description FROM [ SHOW ALL CLUSTER SETTINGS ]",
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "-- Cluster settings:\n")
	for _, r := range rows {
		// The datums should always be DString, but we should be defensive.
		variable, ok1 := r[0].(*tree.DString)
		value, ok2 := r[1].(*tree.DString)
		description, ok3 := r[2].(*tree.DString)
		if ok1 && ok2 && ok3 {
			fmt.Fprintf(w, "--   %s = %s  (%s)\n", *variable, *value, *description)
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
	// Don't display the catalog during the `ALTER TABLE` since the schema file
	// doesn't specify the catalog for its create table statements.
	explicitCatalog := tn.ExplicitCatalog
	tn.ExplicitCatalog = false
	fmt.Fprintf(w, "ALTER TABLE %s INJECT STATISTICS '%s';\n", tn.String(), stats)
	tn.ExplicitCatalog = explicitCatalog
	return nil
}
