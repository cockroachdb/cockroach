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
	"github.com/cockroachdb/cockroach/pkg/sql/colfetcher"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const noPlan = "no plan"

// setExplainBundleResult sets the result of an EXPLAIN ANALYZE (DEBUG)
// statement. warnings will be printed out as is in the CLI.
//
// Note: bundle.insert() must have been called.
//
// Returns an error if information rows couldn't be added to the result.
func setExplainBundleResult(
	ctx context.Context,
	res RestrictedCommandResult,
	bundle diagnosticsBundle,
	execCfg *ExecutorConfig,
	warnings []string,
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
			fmt.Sprintf("Admin UI: %s", execCfg.NodeInfo.AdminURL()),
			fmt.Sprintf("Direct link: %s/_admin/v1/stmtbundle/%d", execCfg.NodeInfo.AdminURL(), bundle.diagID),
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
	if len(warnings) > 0 {
		text = append(text, "")
		text = append(text, warnings...)
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

	// errorStrings are all non-critical errors that we ran into when collecting
	// the bundle. "Non-critical" in this context means that most of the bundle
	// was still collected to be useful, but some parts might be missing.
	errorStrings []string

	// diagID is the diagnostics instance ID, populated by insert().
	diagID stmtdiagnostics.CollectedInstanceID
}

// buildStatementBundle collects metadata related to the planning and execution
// of the statement. It generates a bundle for storage in
// system.statement_diagnostics.
func buildStatementBundle(
	ctx context.Context,
	explainFlags explain.Flags,
	db *kv.DB,
	ie *InternalExecutor,
	stmtRawSQL string,
	plan *planTop,
	planString string,
	trace tracingpb.Recording,
	placeholders *tree.PlaceholderInfo,
	queryErr, payloadErr, commErr error,
	sv *settings.Values,
	c inFlightTraceCollector,
) diagnosticsBundle {
	if plan == nil {
		return diagnosticsBundle{collectionErr: errors.AssertionFailedf("execution terminated early")}
	}
	b := makeStmtBundleBuilder(explainFlags, db, ie, stmtRawSQL, plan, trace, placeholders, sv)

	b.addStatement()
	b.addOptPlans(ctx)
	b.addExecPlan(planString)
	b.addDistSQLDiagrams()
	b.addExplainVec()
	b.addTrace()
	b.addInFlightTrace(c)
	b.addEnv(ctx)
	b.addErrors(queryErr, payloadErr, commErr)

	buf, err := b.finalize()
	if err != nil {
		return diagnosticsBundle{collectionErr: err, errorStrings: b.errorStrings}
	}
	return diagnosticsBundle{zip: buf.Bytes(), errorStrings: b.errorStrings}
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
	req stmtdiagnostics.Request,
) {
	var err error
	bundle.diagID, err = stmtDiagRecorder.InsertStatementDiagnostics(
		ctx,
		diagRequestID,
		req,
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
	flags explain.Flags

	db *kv.DB
	ie *InternalExecutor

	stmt         string
	plan         *planTop
	trace        tracingpb.Recording
	placeholders *tree.PlaceholderInfo
	sv           *settings.Values

	// errorStrings are non-critical errors encountered so far.
	errorStrings []string

	z memzipper.Zipper
}

func makeStmtBundleBuilder(
	flags explain.Flags,
	db *kv.DB,
	ie *InternalExecutor,
	stmtRawSQL string,
	plan *planTop,
	trace tracingpb.Recording,
	placeholders *tree.PlaceholderInfo,
	sv *settings.Values,
) stmtBundleBuilder {
	b := stmtBundleBuilder{
		flags: flags, db: db, ie: ie, plan: plan, trace: trace, placeholders: placeholders, sv: sv,
	}
	b.buildPrettyStatement(stmtRawSQL)
	b.z.Init()
	return b
}

// buildPrettyStatement saves the pretty-printed statement (without any
// placeholder arguments).
func (b *stmtBundleBuilder) buildPrettyStatement(stmtRawSQL string) {
	// If we hit an early error, stmt or stmt.AST might not be initialized yet. In
	// this case use the original raw SQL.
	if b.plan.stmt == nil || b.plan.stmt.AST == nil {
		b.stmt = stmtRawSQL
		// If we're collecting a redacted bundle, redact the raw SQL completely.
		if b.flags.RedactValues && b.stmt != "" {
			b.stmt = string(redact.RedactedMarker())
		}
	} else {
		cfg := tree.DefaultPrettyCfg()
		cfg.UseTabs = false
		cfg.LineWidth = 100
		cfg.TabWidth = 2
		cfg.Simplify = true
		cfg.Align = tree.PrettyNoAlign
		cfg.JSONFmt = true
		cfg.ValueRedaction = b.flags.RedactValues
		b.stmt = cfg.Pretty(b.plan.stmt.AST)

		// If we had ValueRedaction set, Pretty surrounded all constants with
		// redaction markers. We must call Redact to fully redact them.
		if b.flags.RedactValues {
			b.stmt = string(redact.RedactableString(b.stmt).Redact())
		}
	}
	if b.stmt == "" {
		b.stmt = "-- no statement"
	}
}

// addStatement adds the pretty-printed statement in b.stmt as file
// statement.txt.
func (b *stmtBundleBuilder) addStatement() {
	output := b.stmt

	if b.placeholders != nil && len(b.placeholders.Values) != 0 {
		var buf bytes.Buffer
		buf.WriteString(output)
		buf.WriteString("\n\n-- Arguments:\n")
		for i, v := range b.placeholders.Values {
			if b.flags.RedactValues {
				fmt.Fprintf(&buf, "--  %s: %s\n", tree.PlaceholderIdx(i), redact.RedactedMarker())
			} else {
				fmt.Fprintf(&buf, "--  %s: %v\n", tree.PlaceholderIdx(i), v)
			}
		}
		output = buf.String()
	}

	b.z.AddFile("statement.sql", output)
}

// addOptPlans adds the EXPLAIN (OPT) variants as files opt.txt, opt-v.txt,
// opt-vv.txt.
func (b *stmtBundleBuilder) addOptPlans(ctx context.Context) {
	if b.plan.mem == nil || b.plan.mem.RootExpr() == nil {
		// No optimizer plans; an error must have occurred during planning.
		b.z.AddFile("opt.txt", noPlan)
		b.z.AddFile("opt-v.txt", noPlan)
		b.z.AddFile("opt-vv.txt", noPlan)
		return
	}

	formatOptPlan := func(flags memo.ExprFmtFlags) string {
		f := memo.MakeExprFmtCtx(ctx, flags, b.flags.RedactValues, b.plan.mem, b.plan.catalog)
		f.FormatExpr(b.plan.mem.RootExpr())
		output := f.Buffer.String()
		if b.flags.RedactValues {
			output = string(redact.RedactableString(output).Redact())
		}
		return output
	}

	b.z.AddFile("opt.txt", formatOptPlan(memo.ExprFmtHideAll))
	b.z.AddFile("opt-v.txt", formatOptPlan(
		memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes|memo.ExprFmtHideNotVisibleIndexInfo,
	))
	b.z.AddFile("opt-vv.txt", formatOptPlan(memo.ExprFmtHideQualifications|memo.ExprFmtHideNotVisibleIndexInfo))
}

// addExecPlan adds the EXPLAIN (VERBOSE) plan as file plan.txt.
func (b *stmtBundleBuilder) addExecPlan(plan string) {
	if plan == "" {
		plan = "no plan"
	}
	b.z.AddFile("plan.txt", plan)
}

func (b *stmtBundleBuilder) addDistSQLDiagrams() {
	if b.flags.RedactValues {
		return
	}

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
	if len(b.plan.distSQLFlowInfos) == 0 {
		b.z.AddFile("distsql.html", "<body>no execution</body>")
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
	if b.flags.RedactValues {
		return
	}

	traceJSONStr, err := tracing.TraceToJSON(b.trace)
	if err != nil {
		b.z.AddFile("trace.json", err.Error())
	} else {
		b.z.AddFile("trace.json", traceJSONStr)
	}

	// The JSON is not very human-readable, so we include another format too.
	b.z.AddFile("trace.txt", fmt.Sprintf("%s\n\n\n\n%s", b.stmt, b.trace.String()))

	// Note that we're going to include the non-anonymized statement in the trace.
	// But then again, nothing in the trace is anonymized.
	comment := fmt.Sprintf(`This is a trace for SQL statement: %s
This trace can be imported into Jaeger for visualization. From the Jaeger Search screen, select the JSON File.
Jaeger can be started using docker with: docker run -d --name jaeger -p 16686:16686 jaegertracing/all-in-one:1.17
The UI can then be accessed at http://localhost:16686/search`, b.stmt)
	jaegerJSON, err := b.trace.ToJaegerJSON(b.stmt, comment, "")
	if err != nil {
		b.errorStrings = append(b.errorStrings, fmt.Sprintf("error getting jaeger trace: %v", err))
		b.z.AddFile("trace-jaeger.txt", err.Error())
	} else {
		b.z.AddFile("trace-jaeger.json", jaegerJSON)
	}
}

func (b *stmtBundleBuilder) addInFlightTrace(c inFlightTraceCollector) {
	if b.flags.RedactValues {
		return
	}
	for _, trace := range c.trace {
		b.z.AddFile(fmt.Sprintf("inflight-trace-n%d.txt", trace.nodeID), trace.trace)
		b.z.AddFile(fmt.Sprintf("inflight-trace-jaeger-n%d.json", trace.nodeID), trace.jaeger)
	}
	if len(c.trace) == 0 && len(c.errors) > 0 {
		// Include all errors accumulated throughout the in-flight tracing if we
		// weren't able to get even a single trace.
		var sb strings.Builder
		for j, err := range c.errors {
			if j > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(err.Error())
		}
		b.z.AddFile("inflight-trace-errors.txt", sb.String())
	}
	// Include the timeout trace if available.
	for _, trace := range c.timeoutTrace {
		b.z.AddFile(fmt.Sprintf("timeout-trace-n%d.txt", trace.nodeID), trace.trace)
		b.z.AddFile(fmt.Sprintf("timeout-trace-jaeger-n%d.json", trace.nodeID), trace.jaeger)
	}
}

// printError writes the given error string into buf (with a newline appended)
// as well as accumulates the string into b.errorStrings. The method should only
// be used for non-critical errors.
func (b *stmtBundleBuilder) printError(errString string, buf *bytes.Buffer) {
	fmt.Fprintf(buf, errString+"\n")
	b.errorStrings = append(b.errorStrings, errString)
}

func (b *stmtBundleBuilder) addEnv(ctx context.Context) {
	c := makeStmtEnvCollector(ctx, b.ie)

	var buf bytes.Buffer
	if err := c.PrintVersion(&buf); err != nil {
		b.printError(fmt.Sprintf("-- error getting version: %v", err), &buf)
	}
	fmt.Fprintf(&buf, "\n")

	// Show the values of session variables that can impact planning decisions.
	if err := c.PrintSessionSettings(&buf, b.sv); err != nil {
		b.printError(fmt.Sprintf("-- error getting session settings: %v", err), &buf)
	}

	fmt.Fprintf(&buf, "\n")

	if err := c.PrintClusterSettings(&buf); err != nil {
		b.printError(fmt.Sprintf("-- error getting cluster settings: %v", err), &buf)
	}

	b.z.AddFile("env.sql", buf.String())

	mem := b.plan.mem
	if mem == nil {
		// No optimizer plans; an error must have occurred during planning.
		b.z.AddFile("schema.sql", "-- no schema collected\n")
		return
	}
	buf.Reset()

	var tables, sequences, views []tree.TableName
	err := b.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		tables, sequences, views, err = mem.Metadata().AllDataSourceNames(
			ctx, b.plan.catalog, func(ds cat.DataSource) (cat.DataSourceName, error) {
				return b.plan.catalog.fullyQualifiedNameWithTxn(ctx, ds, txn)
			},
		)
		return err
	})
	if err != nil {
		errString := fmt.Sprintf("-- error getting data source names: %v", err)
		b.errorStrings = append(b.errorStrings, errString)
		b.z.AddFile("schema.sql", errString+"\n")
		return
	}

	// Note: we do not shortcut out of this function if there is no table/sequence/view to report:
	// the bundle analysis tool require schema.sql to always be present, even if it's empty.

	first := true
	blankLine := func() {
		if !first {
			buf.WriteByte('\n')
		}
		first = false
	}
	blankLine()
	if err := c.printCreateAllSchemas(&buf); err != nil {
		b.printError(fmt.Sprintf("-- error getting all schemas: %v", err), &buf)
	}
	for i := range sequences {
		blankLine()
		if err := c.PrintCreateSequence(&buf, &sequences[i]); err != nil {
			b.printError(fmt.Sprintf("-- error getting schema for sequence %s: %v", sequences[i].String(), err), &buf)
		}
	}
	// Get all user-defined types. If redaction is a
	blankLine()
	if err := c.PrintCreateEnum(&buf, b.flags.RedactValues); err != nil {
		b.printError(fmt.Sprintf("-- error getting schema for enums: %v", err), &buf)
	}
	if mem.Metadata().HasUserDefinedFunctions() {
		// Get all relevant user-defined functions.
		blankLine()
		if err := c.PrintRelevantCreateUdf(&buf, strings.ToLower(b.stmt), b.flags.RedactValues, &b.errorStrings); err != nil {
			b.printError(fmt.Sprintf("-- error getting schema for udfs: %v", err), &buf)
		}
	}
	for i := range tables {
		blankLine()
		if err := c.PrintCreateTable(&buf, &tables[i], b.flags.RedactValues); err != nil {
			b.printError(fmt.Sprintf("-- error getting schema for table %s: %v", tables[i].String(), err), &buf)
		}
	}
	for i := range views {
		blankLine()
		if err := c.PrintCreateView(&buf, &views[i], b.flags.RedactValues); err != nil {
			b.printError(fmt.Sprintf("-- error getting schema for view %s: %v", views[i].String(), err), &buf)
		}
	}
	if buf.Len() == 0 {
		buf.WriteString("-- there were no objects used in this query\n")
	}
	b.z.AddFile("schema.sql", buf.String())
	for i := range tables {
		buf.Reset()
		hideHistograms := b.flags.RedactValues
		if err := c.PrintTableStats(&buf, &tables[i], hideHistograms); err != nil {
			b.printError(fmt.Sprintf("-- error getting statistics for table %s: %v", tables[i].String(), err), &buf)
		}
		b.z.AddFile(fmt.Sprintf("stats-%s.sql", tables[i].String()), buf.String())
	}
}

func (b *stmtBundleBuilder) addErrors(queryErr, payloadErr, commErr error) {
	if b.flags.RedactValues {
		return
	}

	if queryErr == nil && payloadErr == nil && commErr == nil && len(b.errorStrings) == 0 {
		return
	}
	output := fmt.Sprintf(
		"query error:\n%v\n\npayload error:\n%v\n\ncomm error:\n%v\n\n",
		queryErr, payloadErr, commErr,
	)
	for _, errString := range b.errorStrings {
		output += errString + "\n"
	}
	b.z.AddFile("errors.txt", output)
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

// query is a helper to run a query that returns a single string value.
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

// queryRows is similar to query() for the case when multiple rows with single
// string values can be returned.
func (c *stmtEnvCollector) queryRows(query string) ([]string, error) {
	rows, err := c.ie.QueryBufferedEx(
		c.ctx,
		"stmtEnvCollector",
		nil, /* txn */
		sessiondata.NoSessionDataOverride,
		query,
	)
	if err != nil {
		return nil, err
	}

	var values []string
	for _, row := range rows {
		if len(row) != 1 {
			return nil, errors.AssertionFailedf(
				"expected env query %q to return a single column, returned %d",
				query, len(row),
			)
		}
		s, ok := row[0].(*tree.DString)
		if !ok {
			return nil, errors.AssertionFailedf(
				"expected env query %q to return a DString, returned %T",
				query, row[0],
			)
		}
		values = append(values, string(*s))
	}

	return values, nil
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
//
//	-- Version: CockroachDB CCL v20.1.0 ...
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
func (c *stmtEnvCollector) PrintSessionSettings(w io.Writer, sv *settings.Values) error {
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

	datestyleConv := func(enumVal string) string {
		n, err := strconv.ParseInt(enumVal, 10, 32)
		if err != nil || n < 0 || n >= int64(len(dateStyleEnumMap)) {
			return enumVal
		}
		return dateStyleEnumMap[n]
	}

	intervalstyleConv := func(enumVal string) string {
		n, err := strconv.ParseInt(enumVal, 10, 32)
		if err != nil || n < 0 || n >= int64(len(duration.IntervalStyle_name)) {
			return enumVal
		}
		return strings.ToLower(duration.IntervalStyle(n).String())
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
		{sessionSetting: "allow_prepare_as_opt_plan"},
		{sessionSetting: "cost_scans_with_default_col_size", clusterSetting: costScansWithDefaultColSize, convFunc: boolToOnOff},
		{sessionSetting: "datestyle", clusterSetting: dateStyle, convFunc: datestyleConv},
		{sessionSetting: "default_int_size", clusterSetting: defaultIntSize},
		{sessionSetting: "default_transaction_priority"},
		{sessionSetting: "default_transaction_quality_of_service"},
		{sessionSetting: "default_transaction_read_only"},
		{sessionSetting: "default_transaction_use_follower_reads"},
		{sessionSetting: "direct_columnar_scans_enabled", clusterSetting: colfetcher.DirectScansEnabled, convFunc: boolToOnOff},
		{sessionSetting: "disallow_full_table_scans", clusterSetting: disallowFullTableScans, convFunc: boolToOnOff},
		{sessionSetting: "distsql", clusterSetting: DistSQLClusterExecMode, convFunc: distsqlConv},
		{sessionSetting: "enable_implicit_select_for_update", clusterSetting: implicitSelectForUpdateClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "enable_implicit_transaction_for_batch_statements"},
		{sessionSetting: "enable_insert_fast_path", clusterSetting: insertFastPathClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "enable_multiple_modifications_of_table"},
		{sessionSetting: "enable_zigzag_join", clusterSetting: zigzagJoinClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "expect_and_ignore_not_visible_columns_in_copy"},
		{sessionSetting: "intervalstyle", clusterSetting: intervalStyle, convFunc: intervalstyleConv},
		{sessionSetting: "large_full_scan_rows", clusterSetting: largeFullScanRows},
		{sessionSetting: "locality_optimized_partitioned_index_scan", clusterSetting: localityOptimizedSearchMode, convFunc: boolToOnOff},
		{sessionSetting: "null_ordered_last"},
		{sessionSetting: "on_update_rehome_row_enabled", clusterSetting: onUpdateRehomeRowEnabledClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "opt_split_scan_limit"},
		{sessionSetting: "optimizer_use_forecasts", convFunc: boolToOnOff},
		{sessionSetting: "optimizer_use_histograms", clusterSetting: optUseHistogramsClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "optimizer_use_multicol_stats", clusterSetting: optUseMultiColStatsClusterMode, convFunc: boolToOnOff},
		{sessionSetting: "optimizer_use_not_visible_indexes"},
		{sessionSetting: "pg_trgm.similarity_threshold"},
		{sessionSetting: "prefer_lookup_joins_for_fks", clusterSetting: preferLookupJoinsForFKs, convFunc: boolToOnOff},
		{sessionSetting: "propagate_input_ordering", clusterSetting: propagateInputOrdering, convFunc: boolToOnOff},
		{sessionSetting: "reorder_joins_limit", clusterSetting: ReorderJoinsLimitClusterValue},
		{sessionSetting: "sql_safe_updates"},
		{sessionSetting: "testing_optimizer_cost_perturbation"},
		{sessionSetting: "testing_optimizer_disable_rule_probability"},
		{sessionSetting: "testing_optimizer_random_seed"},
		{sessionSetting: "timezone"},
		{sessionSetting: "unbounded_parallel_scans"},
		{sessionSetting: "unconstrained_non_covering_index_scan_enabled"},
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
			if ok, v, _ := getSessionVar(s.sessionSetting, true); ok {
				if v.GlobalDefault != nil {
					def = v.GlobalDefault(sv)
				}
			}
		} else {
			def = s.clusterSetting.EncodedDefault()
			if buildutil.CrdbTestBuild {
				// In test builds we might randomize some setting defaults, so
				// we need to override them to make the tests deterministic.
				switch s.sessionSetting {
				case "direct_columnar_scans_enabled":
					def = "false"
				}
			}
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
	// TODO(michae2): We should also query system.database_role_settings.

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

func (c *stmtEnvCollector) PrintCreateTable(
	w io.Writer, tn *tree.TableName, redactValues bool,
) error {
	var formatOption string
	if redactValues {
		formatOption = " WITH REDACT"
	}
	createStatement, err := c.query(
		fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s%s]", tn.String(), formatOption),
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

func (c *stmtEnvCollector) PrintCreateEnum(w io.Writer, redactValues bool) error {
	qry := "SELECT create_statement FROM [SHOW CREATE ALL TYPES]"
	if redactValues {
		qry = "SELECT crdb_internal.redact(crdb_internal.redactable_sql_constants(create_statement)) FROM [SHOW CREATE ALL TYPES]"

	}
	createStatement, err := c.queryRows(qry)
	if err != nil {
		return err
	}
	for _, cs := range createStatement {
		fmt.Fprintf(w, "%s\n", cs)
	}
	return nil
}

func (c *stmtEnvCollector) PrintRelevantCreateUdf(
	w io.Writer, stmt string, redactValues bool, errorStrings *[]string,
) error {
	// The select function_name returns a DOidWrapper,
	// we need to cast it to string for queryRows function to process.
	// TODO: consider getting the udf sql body statements from the memo metadata.
	functionNameQuery := "SELECT function_name::STRING as function_name_str FROM [SHOW FUNCTIONS]"
	udfNames, err := c.queryRows(functionNameQuery)
	if err != nil {
		return err
	}
	for _, name := range udfNames {
		if strings.Contains(stmt, name) {
			createFunctionQuery := fmt.Sprintf(
				"SELECT create_statement FROM [ SHOW CREATE FUNCTION \"%s\" ]", name,
			)
			if redactValues {
				createFunctionQuery = fmt.Sprintf(
					"SELECT crdb_internal.redact(crdb_internal.redactable_sql_constants(create_statement)) FROM [ SHOW CREATE FUNCTION \"%s\" ]", name,
				)
			}
			createStatement, err := c.query(createFunctionQuery)
			if err != nil {
				errString := fmt.Sprintf("-- error getting user defined function %s: %s", name, err)
				fmt.Fprint(w, errString+"\n")
				*errorStrings = append(*errorStrings, errString)
				continue
			}
			fmt.Fprintf(w, "%s\n", createStatement)
		}
	}
	return nil
}

func (c *stmtEnvCollector) PrintCreateView(
	w io.Writer, tn *tree.TableName, redactValues bool,
) error {
	var formatOption string
	if redactValues {
		formatOption = " WITH REDACT"
	}
	createStatement, err := c.query(fmt.Sprintf(
		"SELECT create_statement FROM [SHOW CREATE VIEW %s%s]", tn.String(), formatOption,
	))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) printCreateAllSchemas(w io.Writer) error {
	createAllSchemas, err := c.queryRows("SHOW CREATE ALL SCHEMAS;")
	if err != nil {
		return err
	}
	for _, r := range createAllSchemas {
		if r == "CREATE SCHEMA "+catconstants.PublicSchemaName+";" {
			// The public schema is always present, so exclude it to ease the
			// recreation of the bundle.
			continue
		}
		fmt.Fprintf(w, "%s\n", r)
	}
	return nil
}

func (c *stmtEnvCollector) PrintTableStats(
	w io.Writer, tn *tree.TableName, hideHistograms bool,
) error {
	var maybeRemoveHistoBuckets string
	if hideHistograms {
		maybeRemoveHistoBuckets = ` - 'histo_buckets' - 'histo_version' || '{"histo_col_type": ""}'`
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
