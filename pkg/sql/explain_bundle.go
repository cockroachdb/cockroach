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
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
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
	p *planner,
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
	b, err := makeStmtBundleBuilder(explainFlags, db, p, ie, stmtRawSQL, plan, trace, placeholders, sv)
	if err != nil {
		return diagnosticsBundle{collectionErr: err}
	}

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
	p  *planner
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
	p *planner,
	ie *InternalExecutor,
	stmtRawSQL string,
	plan *planTop,
	trace tracingpb.Recording,
	placeholders *tree.PlaceholderInfo,
	sv *settings.Values,
) (stmtBundleBuilder, error) {
	b := stmtBundleBuilder{
		flags: flags, db: db, p: p, ie: ie, plan: plan, trace: trace, placeholders: placeholders, sv: sv,
	}
	err := b.buildPrettyStatement(stmtRawSQL)
	if err != nil {
		return stmtBundleBuilder{}, err
	}
	b.z.Init()
	return b, nil
}

// buildPrettyStatement saves the pretty-printed statement (without any
// placeholder arguments).
func (b *stmtBundleBuilder) buildPrettyStatement(stmtRawSQL string) error {
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
		var err error
		b.stmt, err = cfg.Pretty(b.plan.stmt.AST)
		if errors.Is(err, pretty.ErrPrettyMaxRecursionDepthExceeded) {
			// Use the raw statement string if pretty-printing fails.
			b.stmt = stmtRawSQL
			// If we're collecting a redacted bundle, redact the raw SQL
			// completely.
			if b.flags.RedactValues && b.stmt != "" {
				b.stmt = string(redact.RedactedMarker())
			}
		} else if err != nil {
			return err
		}

		// If we had ValueRedaction set, Pretty surrounded all constants with
		// redaction markers. We must call Redact to fully redact them.
		if b.flags.RedactValues {
			b.stmt = string(redact.RedactableString(b.stmt).Redact())
		}
	}
	if b.stmt == "" {
		b.stmt = "-- no statement"
	}
	return nil
}

// ReplacePlaceholdersWithValuesForBundle takes the contents of statement.sql
// from the bundle and substitutes all placeholders with their values.
func ReplacePlaceholdersWithValuesForBundle(
	bundleStmt string,
) (filledStmt string, numPlaceholders int, _ error) {
	stmtComponents := strings.Split(bundleStmt, "-- Arguments:")
	switch len(stmtComponents) {
	case 1:
		// There are no placeholders in the statement, so we just use it
		// directly.
		filledStmt = stmtComponents[0]
	case 2:
		// We have placeholders in the stmt, so we'll replace them with
		// their values.
		values := strings.Split(stmtComponents[1], "\n")
		// Remove empty lines at the beginning and at the end.
		for values[0] == "" {
			values = values[1:]
		}
		for values[len(values)-1] == "" {
			values = values[:len(values)-1]
		}
		filledStmt = stmtComponents[0]
		numPlaceholders = len(values)
		reg := regexp.MustCompile(`.*\$(\d+): (.+)`)
		// Iterate backwards so that we don't have collisions like $1 and
		// $10 being replaced by the same value.
		for i := len(values) - 1; i >= 0; i-- {
			// Each string is currently of the form
			//   --  $1: '2024-01-23 20:31:00.739925'
			// so we want to extract the placeholder index as well as the
			// placeholder value.
			value := strings.TrimSpace(values[i])
			matches := reg.FindStringSubmatch(value)
			if len(matches) != 3 {
				return "", 0, errors.Newf("couldn't parse the placeholder value string: %q\n%v", value, matches)
			}
			placeholderIdx, err := strconv.Atoi(matches[1])
			if err != nil {
				return "", 0, errors.Wrapf(err, "couldn't parse the placeholder value string: %q", value)
			}
			filledStmt = strings.ReplaceAll(filledStmt, fmt.Sprintf("$%d", placeholderIdx), matches[2])
		}
	default:
		return "", 0, errors.Newf("unexpected number of parts when splitting statement.sql file: expected 1 or 2, found %d", len(stmtComponents))
	}
	return strings.TrimSpace(filledStmt), numPlaceholders, nil
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
		// Also add a couple of helpful files for ease of reproduction:
		// - one file with PREPARE and EXECUTE
		// - another file with placeholders replaced with their values.
		buf.Reset()
		buf.WriteString("PREPARE p AS ")
		buf.WriteString(b.stmt)
		buf.WriteString(";\n\nEXECUTE p (")
		for i, v := range b.placeholders.Values {
			if i > 0 {
				fmt.Fprintf(&buf, ",")
			}
			if b.flags.RedactValues {
				fmt.Fprintf(&buf, "\n  /* %s */ %s", tree.PlaceholderIdx(i), redact.RedactedMarker())
			} else {
				fmt.Fprintf(&buf, "\n  /* %s */ %v", tree.PlaceholderIdx(i), v)
			}
		}
		buf.WriteString("\n);\n")
		b.z.AddFile("statement-prepared.sql", buf.String())
		stmtNoPlaceholders, _, err := ReplacePlaceholdersWithValuesForBundle(output)
		if err == nil {
			b.z.AddFile("statement-no-placeholders.sql", stmtNoPlaceholders)
		}
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
		memo.ExprFmtHideQualifications|memo.ExprFmtHideScalars|memo.ExprFmtHideTypes|memo.ExprFmtHideNotVisibleIndexInfo|memo.ExprFmtHideFastPathChecks,
	))
	b.z.AddFile("opt-vv.txt", formatOptPlan(memo.ExprFmtHideQualifications|memo.ExprFmtHideNotVisibleIndexInfo))
}

// addExecPlan adds the EXPLAIN (VERBOSE) plan as file plan.txt.
func (b *stmtBundleBuilder) addExecPlan(plan string) {
	if plan == "" {
		plan = noPlan
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
	c := makeStmtEnvCollector(ctx, b.p, b.ie)

	var buf bytes.Buffer
	if err := c.PrintVersion(&buf); err != nil {
		b.printError(fmt.Sprintf("-- error getting version: %v", err), &buf)
	}
	fmt.Fprintf(&buf, "\n")

	// Show the values of session variables that have values different from the
	// cluster defaults.
	if err := c.PrintSessionSettings(&buf, b.sv, false /* all */); err != nil {
		b.printError(fmt.Sprintf("-- error getting session settings: %v", err), &buf)
	}

	fmt.Fprintf(&buf, "\n")

	if err := c.PrintClusterSettings(&buf, false /* all */); err != nil {
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

	// TODO(#27611): when we support stats on virtual tables, we'll need to
	// update this logic to not include virtual tables into schema.sql but still
	// create stats files for them.
	var tables, sequences, views []tree.TableName
	err := b.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		tables, sequences, views, err = mem.Metadata().AllDataSourceNames(
			ctx, b.plan.catalog, func(ds cat.DataSource) (cat.DataSourceName, error) {
				return b.plan.catalog.fullyQualifiedNameWithTxn(ctx, ds, txn)
			},
			false, /* includeVirtualTables */
		)
		return err
	})
	if err != nil {
		errString := fmt.Sprintf("-- error getting data source names: %v", err)
		b.errorStrings = append(b.errorStrings, errString)
		b.z.AddFile("schema.sql", errString+"\n")
		return
	}

	dbNames := make(map[string]struct{})
	schemaNames := make(map[string]struct{})
	collectDBAndSchemaNames := func(dataSources []tree.TableName) {
		for _, ds := range dataSources {
			dbNames[ds.CatalogName.String()] = struct{}{}
			schemaNames[fmt.Sprintf("%s.%s", ds.CatalogName.String(), ds.SchemaName.String())] = struct{}{}
		}
	}
	collectDBAndSchemaNames(tables)
	collectDBAndSchemaNames(sequences)
	collectDBAndSchemaNames(views)

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
	c.printCreateAllDatabases(&buf, dbNames)
	if err := c.printCreateAllSchemas(&buf, schemaNames); err != nil {
		b.printError(fmt.Sprintf("-- error getting all schemas: %v", err), &buf)
	}
	for i := range sequences {
		blankLine()
		if err := c.PrintCreateSequence(&buf, &sequences[i]); err != nil {
			b.printError(fmt.Sprintf("-- error getting schema for sequence %s: %v", sequences[i].FQString(), err), &buf)
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
		err = c.PrintRelevantCreateRoutine(
			&buf, strings.ToLower(b.stmt), b.flags.RedactValues, &b.errorStrings, false, /* procedure */
		)
		if err != nil {
			b.printError(fmt.Sprintf("-- error getting schema for udfs: %v", err), &buf)
		}
	}
	if call, ok := mem.RootExpr().(*memo.CallExpr); ok {
		// Currently, a stored procedure can only be called from a CALL statement,
		// which can only be the root expression.
		if proc, ok := call.Proc.(*memo.UDFCallExpr); ok {
			blankLine()
			err = c.PrintRelevantCreateRoutine(
				&buf, strings.ToLower(proc.Def.Name), b.flags.RedactValues, &b.errorStrings, true, /* procedure */
			)
			if err != nil {
				b.printError(fmt.Sprintf("-- error getting schema for procedure: %v", err), &buf)
			}
		} else {
			b.printError("-- unexpected input expression for CALL statement", &buf)
		}
	}
	for i := range tables {
		blankLine()
		if err := c.PrintCreateTable(&buf, &tables[i], b.flags.RedactValues); err != nil {
			b.printError(fmt.Sprintf("-- error getting schema for table %s: %v", tables[i].FQString(), err), &buf)
		}
	}
	for i := range views {
		blankLine()
		if err := c.PrintCreateView(&buf, &views[i], b.flags.RedactValues); err != nil {
			b.printError(fmt.Sprintf("-- error getting schema for view %s: %v", views[i].FQString(), err), &buf)
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
			b.printError(fmt.Sprintf("-- error getting statistics for table %s: %v", tables[i].FQString(), err), &buf)
		}
		b.z.AddFile(fmt.Sprintf("stats-%s.sql", tables[i].FQString()), buf.String())
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
	p   *planner
	ie  *InternalExecutor
}

func makeStmtEnvCollector(ctx context.Context, p *planner, ie *InternalExecutor) stmtEnvCollector {
	return stmtEnvCollector{ctx: ctx, p: p, ie: ie}
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

// makeSingleLine replaces all control characters with a single space. This is
// needed so that session variables and cluster settings would fit on a single
// line (possibly commented out).
func makeSingleLine(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
}

// PrintSessionSettings appends information about all session variables that
// differ from their cluster defaults.
//
// If all is true, then all variables are included.
func (c *stmtEnvCollector) PrintSessionSettings(w io.Writer, sv *settings.Values, all bool) error {
	for _, varName := range varNames {
		gen := varGen[varName]
		value, err := gen.Get(&c.p.extendedEvalCtx, c.p.Txn())
		if err != nil {
			return err
		}
		value = makeSingleLine(value)
		if gen.Set == nil && gen.RuntimeSet == nil && gen.SetWithPlanner == nil {
			// Read-only variable.
			if buildutil.CrdbTestBuild {
				// Skip all read-only variables in tests.
				continue
			}
			if _, skip := skipReadOnlySessionVar[varName]; skip {
				// Skip it since its value is unlikely to be useful.
				continue
			}
			fmt.Fprintf(w, "-- read-only %s = %s\n", varName, value)
			continue
		}
		// All writable variables must have GlobalDefault set.
		defaultValue := makeSingleLine(gen.GlobalDefault(sv))
		skip := value == defaultValue
		if buildutil.CrdbTestBuild {
			// In test builds we might randomize some setting defaults, so
			// we need to ignore them to make the tests deterministic.
			switch varName {
			case "direct_columnar_scans_enabled":
				skip = true
			}
		}
		var commentOut bool
		switch varName {
		case "application_name", "database":
			// These show up in the logic tests, but it's unclear whether we
			// want to apply them when recreating the bundle, so we'll comment
			// them out.
			commentOut = true
		case "idle_in_session_timeout", "idle_in_transaction_session_timeout",
			"idle_session_timeout", "lock_timeout", "statement_timeout", "transaction_timeout":
			// Defaults for timeout settings are of the duration type (i.e.
			// "0s"), so we'll parse it to extract the number of milliseconds
			// (which is what the session variable uses).
			d, err := time.ParseDuration(defaultValue)
			if err != nil {
				return err
			}
			defaultValue = strconv.Itoa(int(d.Milliseconds()))
			if value == defaultValue {
				// We also need to do an additional check to see whether the
				// variable actually uses the default, and we'll skip recording
				// it if so.
				skip = true
			}
		}
		if skip && !all {
			continue
		}
		formatStr := `SET %s = %s;  -- default value: %s`
		if _, ok := sessionVarNeedsQuotes[varName]; ok {
			formatStr = `SET %s = '%s';  -- default value: %s`
		}
		if commentOut {
			formatStr = "-- " + formatStr
		}
		if value == "" {
			// Need a special case for empty strings to make the SET statement
			// parsable.
			value = "''"
		}
		fmt.Fprintf(w, formatStr+"\n", varName, value, defaultValue)
	}
	return nil
}

// PrintClusterSettings appends information about all cluster settings that
// differ from their binary defaults.
//
// If all is true, then all settings are included.
func (c *stmtEnvCollector) PrintClusterSettings(w io.Writer, all bool) error {
	// TODO(michae2): We should also query system.database_role_settings.

	var suffix string
	if !all {
		suffix = " WHERE value != default_value"
	}
	rows, err := c.ie.QueryBufferedEx(
		c.ctx,
		"stmtEnvCollector",
		nil, /* txn */
		sessiondata.NoSessionDataOverride,
		fmt.Sprintf(`SELECT variable, value, default_value FROM crdb_internal.cluster_settings%s`, suffix),
	)
	if err != nil {
		return err
	}
	for _, r := range rows {
		// The datums should always be DString, but we should be defensive.
		setting, ok1 := r[0].(*tree.DString)
		value, ok2 := r[1].(*tree.DString)
		def, ok3 := r[2].(*tree.DString)
		if ok1 && ok2 && ok3 {
			var skip bool
			// Ignore some settings that might differ from their default values
			// but aren't useful in stmt bundles.
			switch *setting {
			case "cluster.secret", "diagnostics.reporting.enabled", "version":
				skip = true
			}
			if buildutil.CrdbTestBuild {
				switch *setting {
				case "sql.distsql.direct_columnar_scans.enabled":
					// This setting's default is randomized in test builds.
					skip = true
				case "bulkio.import.constraint_validation.unsafe.enabled",
					"kv.raft_log.synchronization.unsafe.disabled":
					// These settings are marked as "unsafe", so skip them in
					// the tests.
					skip = true
				}
			}
			if skip {
				continue
			}
			formatStr := `SET CLUSTER SETTING %s = %s;  -- default value: %s`
			if _, ok := clusterSettingNeedsQuotes[string(*setting)]; ok {
				formatStr = `SET CLUSTER SETTING %s = '%s';  -- default value: %s`
			}
			fmt.Fprintf(
				w, formatStr+"\n", *setting, makeSingleLine(string(*value)), makeSingleLine(string(*def)),
			)
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
		fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s%s]", tn.FQString(), formatOption),
	)
	// We need to replace schema.table_name in the create statement with the fully
	// qualified table name.
	createStatement = strings.Replace(createStatement,
		fmt.Sprintf("%s.%s", tn.SchemaName, tn.Table()), tn.FQString(), 1)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) PrintCreateSequence(w io.Writer, tn *tree.TableName) error {
	createStatement, err := c.query(fmt.Sprintf(
		"SELECT create_statement FROM [SHOW CREATE SEQUENCE %s]", tn.FQString(),
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

func (c *stmtEnvCollector) PrintRelevantCreateRoutine(
	w io.Writer, stmt string, redactValues bool, errorStrings *[]string, procedure bool,
) error {
	// The select function_name returns a DOidWrapper,
	// we need to cast it to string for queryRows function to process.
	// TODO(#104976): consider getting the udf sql body statements from the memo metadata.
	var routineTypeName, routineNameQuery string
	if procedure {
		routineTypeName = "PROCEDURE"
		routineNameQuery = "SELECT procedure_name::STRING as procedure_name_str FROM [SHOW PROCEDURES]"
	} else {
		routineTypeName = "FUNCTION"
		routineNameQuery = "SELECT function_name::STRING as function_name_str FROM [SHOW FUNCTIONS]"
	}
	routineNames, err := c.queryRows(routineNameQuery)
	if err != nil {
		return err
	}
	for _, name := range routineNames {
		if strings.Contains(stmt, name) {
			createRoutineQuery := fmt.Sprintf(
				"SELECT create_statement FROM [ SHOW CREATE %s \"%s\" ]", routineTypeName, name,
			)
			if redactValues {
				createRoutineQuery = fmt.Sprintf(
					"SELECT crdb_internal.redact(crdb_internal.redactable_sql_constants(create_statement)) FROM [ SHOW CREATE %s \"%s\" ]",
					routineTypeName, name,
				)
			}
			createStatement, err := c.query(createRoutineQuery)
			if err != nil {
				var errString string
				if procedure {
					errString = fmt.Sprintf("-- error getting stored procedure %s: %s", name, err)
				} else {
					errString = fmt.Sprintf("-- error getting user defined function %s: %s", name, err)
				}
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
		"SELECT create_statement FROM [SHOW CREATE VIEW %s%s]", tn.FQString(), formatOption,
	))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "%s;\n", createStatement)
	return nil
}

func (c *stmtEnvCollector) printCreateAllDatabases(w io.Writer, dbNames map[string]struct{}) {
	for db := range dbNames {
		switch db {
		case catalogkeys.DefaultDatabaseName, catalogkeys.PgDatabaseName, catconstants.SystemDatabaseName:
			// The default, postgres, and system databases are always present, so
			// exclude them to ease the recreation of the bundle.
			continue
		}
		fmt.Fprintf(w, "CREATE DATABASE %s;\n", db)
	}
}

func (c *stmtEnvCollector) printCreateAllSchemas(
	w io.Writer, schemaNames map[string]struct{},
) error {
	for schema := range schemaNames {
		_, schemaOnly, found := strings.Cut(schema, ".")
		if !found {
			return errors.AssertionFailedf("expected schema name to be qualified with DB name")
		}
		switch schemaOnly {
		case catconstants.PublicSchemaName,
			catconstants.InformationSchemaName,
			catconstants.CRDBInternalSchemaName,
			catconstants.PgCatalogName,
			catconstants.PgExtensionSchemaName:
			// The public and virtual schemas are always present, so
			// exclude them to ease the recreation of the bundle.
			continue
		}
		fmt.Fprintf(w, "CREATE SCHEMA %s;\n", schema)
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
		maybeRemoveHistoBuckets, tn.FQString(),
	))
	if err != nil {
		return err
	}

	stats = strings.Replace(stats, "'", "''", -1)
	fmt.Fprintf(w, "ALTER TABLE %s INJECT STATISTICS '%s';\n", tn.FQString(), stats)
	return nil
}

// skipReadOnlySessionVar contains all read-only session variables that are
// explicitly excluded from env.sql of the bundle (they were deemed unlikely to
// be useful in investigations).
var skipReadOnlySessionVar = map[string]struct{}{
	"crdb_version":          {}, // version is included separately
	"integer_datetimes":     {},
	"lc_collate":            {},
	"lc_ctype":              {},
	"max_connections":       {},
	"max_identifier_length": {},
	"max_index_keys":        {},
	"server_encoding":       {},
	"server_version":        {},
	"server_version_num":    {},
	"session_authorization": {},
	"session_id":            {},
	"session_user":          {},
	"system_identity":       {},
	"tracing":               {},
	"virtual_cluster_name":  {},
}

// sessionVarNeedsQuotes contains all writable session variables that have
// values that need single quotes around them in SET statements.
var sessionVarNeedsQuotes = map[string]struct{}{
	"datestyle":                                   {},
	"distsql_workmem":                             {},
	"index_join_streamer_batch_size":              {},
	"join_reader_index_join_strategy_batch_size":  {},
	"join_reader_no_ordering_strategy_batch_size": {},
	"join_reader_ordering_strategy_batch_size":    {},
	"lc_messages":                                 {},
	"lc_monetary":                                 {},
	"lc_numeric":                                  {},
	"lc_time":                                     {},
	"password_encryption":                         {},
	"prepared_statements_cache_size":              {},
	"timezone":                                    {},
}

// clusterSettingNeedsQuotes contains all cluster settings that have values that
// need single quotes around them in SET CLUSTER SETTING statements.
var clusterSettingNeedsQuotes = map[string]struct{}{
	"admission.elastic_cpu.scheduler_latency_target":                   {},
	"admission.epoch_lifo.epoch_closing_delta_duration":                {},
	"admission.epoch_lifo.epoch_duration":                              {},
	"admission.epoch_lifo.queue_delay_threshold_to_switch_to_lifo":     {},
	"admission.replication_control.range_sequencer_gc_threshold":       {},
	"backup.restore.online_worker_count":                               {},
	"backup.restore_span.online_target_size":                           {},
	"backup.restore_span.target_size":                                  {},
	"bulkio.backup.checkpoint_interval":                                {},
	"bulkio.backup.file_size":                                          {},
	"bulkio.backup.merge_file_buffer_size":                             {},
	"bulkio.backup.read_retry_delay":                                   {},
	"bulkio.backup.read_timeout":                                       {},
	"bulkio.backup.read_with_priority_after":                           {},
	"bulkio.import.constraint_validation.unsafe.enabled":               {},
	"bulkio.import.replan_flow_frequency":                              {},
	"bulkio.import.retry_duration":                                     {},
	"bulkio.index_backfill.checkpoint_interval":                        {},
	"bulkio.ingest.flush_delay":                                        {},
	"physical_replication.consumer.cutover_signal_poll_interval":       {},
	"physical_replication.consumer.ingest_range_keys_as_writes":        {},
	"physical_replication.consumer.kv_buffer_size":                     {},
	"physical_replication.consumer.minimum_flush_interval":             {},
	"physical_replication.consumer.range_key_buffer_size":              {},
	"changefeed.aggregator.heartbeat":                                  {},
	"changefeed.cdcevent.trace_kv.log_frequency":                       {},
	"changefeed.cpu.per_event_consumer_worker_allocation":              {},
	"changefeed.cpu.sink_encoding_allocation":                          {},
	"changefeed.experimental_poll_interval":                            {},
	"changefeed.frontier_checkpoint_frequency":                         {},
	"changefeed.frontier_checkpoint_max_bytes":                         {},
	"changefeed.frontier_highwater_lag_checkpoint_threshold":           {},
	"changefeed.auto_idle.timeout":                                     {},
	"changefeed.memory.per_changefeed_limit":                           {},
	"changefeed.min_highwater_advance":                                 {},
	"changefeed.node_throttle_config":                                  {},
	"changefeed.protect_timestamp.max_age":                             {},
	"changefeed.protect_timestamp_interval":                            {},
	"changefeed.schema_feed.read_with_priority_after":                  {},
	"changefeed.slow_span_log_threshold":                               {},
	"changefeed.telemetry.continuous_logging.interval":                 {},
	"cloudstorage.azure.read.node_burst_limit":                         {},
	"cloudstorage.azure.read.node_rate_limit":                          {},
	"cloudstorage.azure.write.node_burst_limit":                        {},
	"cloudstorage.azure.write.node_rate_limit":                         {},
	"cloudstorage.gs.chunking.per_chunk_retry.timeout":                 {},
	"cloudstorage.gs.read.node_burst_limit":                            {},
	"cloudstorage.gs.read.node_rate_limit":                             {},
	"cloudstorage.gs.write.node_burst_limit":                           {},
	"cloudstorage.gs.write.node_rate_limit":                            {},
	"cloudstorage.http.custom_ca":                                      {},
	"cloudstorage.http.read.node_burst_limit":                          {},
	"cloudstorage.http.read.node_rate_limit":                           {},
	"cloudstorage.http.write.node_burst_limit":                         {},
	"cloudstorage.http.write.node_rate_limit":                          {},
	"cloudstorage.nodelocal.read.node_burst_limit":                     {},
	"cloudstorage.nodelocal.read.node_rate_limit":                      {},
	"cloudstorage.nodelocal.write.node_burst_limit":                    {},
	"cloudstorage.nodelocal.write.node_rate_limit":                     {},
	"cloudstorage.nullsink.read.node_burst_limit":                      {},
	"cloudstorage.nullsink.read.node_rate_limit":                       {},
	"cloudstorage.nullsink.write.node_burst_limit":                     {},
	"cloudstorage.nullsink.write.node_rate_limit":                      {},
	"cloudstorage.s3.read.node_burst_limit":                            {},
	"cloudstorage.s3.read.node_rate_limit":                             {},
	"cloudstorage.s3.write.node_burst_limit":                           {},
	"cloudstorage.s3.write.node_rate_limit":                            {},
	"cloudstorage.timeout":                                             {},
	"cloudstorage.userfile.read.node_burst_limit":                      {},
	"cloudstorage.userfile.read.node_rate_limit":                       {},
	"cloudstorage.userfile.write.node_burst_limit":                     {},
	"cloudstorage.userfile.write.node_rate_limit":                      {},
	"cloudstorage.write_chunk.size":                                    {},
	"cluster.label":                                                    {},
	"cluster.organization":                                             {},
	"cluster.preserve_downgrade_option":                                {},
	"diagnostics.forced_sql_stat_reset.interval":                       {},
	"diagnostics.reporting.interval":                                   {},
	"enterprise.license":                                               {},
	"external.graphite.endpoint":                                       {},
	"external.graphite.interval":                                       {},
	"jobs.debug.pausepoints":                                           {},
	"jobs.execution_errors.max_entry_size":                             {},
	"jobs.metrics.interval.poll":                                       {},
	"jobs.registry.interval.adopt":                                     {},
	"jobs.registry.interval.cancel":                                    {},
	"jobs.registry.interval.gc":                                        {},
	"jobs.registry.retry.initial_delay":                                {},
	"jobs.registry.retry.max_delay":                                    {},
	"jobs.retention_time":                                              {},
	"jobs.scheduler.pace":                                              {},
	"jobs.scheduler.schedule_execution.timeout":                        {},
	"keyvisualizer.sample_interval":                                    {},
	"kv.allocator.load_based_rebalancing":                              {},
	"kv.allocator.load_based_rebalancing_interval":                     {},
	"kv.allocator.min_io_overload_lease_shed_interval":                 {},
	"kv.allocator.min_lease_transfer_interval":                         {},
	"kv.bulk_ingest.batch_size":                                        {},
	"kv.bulk_ingest.index_buffer_size":                                 {},
	"kv.bulk_ingest.max_index_buffer_size":                             {},
	"kv.bulk_ingest.max_pk_buffer_size":                                {},
	"kv.bulk_ingest.pk_buffer_size":                                    {},
	"kv.bulk_ingest.stream_external_ssts.suffix_cache_size":            {},
	"kv.bulk_io_write.max_rate":                                        {},
	"kv.bulk_io_write.small_write_size":                                {},
	"kv.bulk_sst.max_allowed_overage":                                  {},
	"kv.bulk_sst.sync_size":                                            {},
	"kv.bulk_sst.target_size":                                          {},
	"kv.closed_timestamp.lead_for_global_reads_override":               {},
	"kv.closed_timestamp.propagation_slack":                            {},
	"kv.closed_timestamp.side_transport_interval":                      {},
	"kv.closed_timestamp.target_duration":                              {},
	"kv.concurrency.long_latch_hold_duration":                          {},
	"kv.dist_sender.circuit_breaker.cancellation.write_grace_period":   {},
	"kv.dist_sender.circuit_breaker.probe.interval":                    {},
	"kv.dist_sender.circuit_breaker.probe.threshold":                   {},
	"kv.dist_sender.circuit_breaker.probe.timeout":                     {},
	"kv.dist_sender.circuit_breakers.mode":                             {},
	"kv.gc.lock_age_threshold":                                         {},
	"kv.gc.txn_cleanup_threshold":                                      {},
	"kv.lease_transfer_read_summary.global_budget":                     {},
	"kv.lease_transfer_read_summary.local_budget":                      {},
	"kv.lock_table.deadlock_detection_or_liveness_push_delay":          {},
	"kv.migration.migrate_application.timeout":                         {},
	"kv.mvcc_gc.queue_high_priority_interval":                          {},
	"kv.mvcc_gc.queue_interval":                                        {},
	"kv.prober.planner.scan_meta2.timeout":                             {},
	"kv.prober.quarantine.write.interval":                              {},
	"kv.prober.read.interval":                                          {},
	"kv.prober.read.timeout":                                           {},
	"kv.prober.write.interval":                                         {},
	"kv.prober.write.timeout":                                          {},
	"kv.protectedts.poll_interval":                                     {},
	"kv.protectedts.reconciliation.interval":                           {},
	"kv.query_resolved_timestamp.intent_cleanup_age":                   {},
	"kv.queue.process.guaranteed_time_budget":                          {},
	"kv.raft.command.max_size":                                         {},
	"kv.raft.command.target_batch_size":                                {},
	"kv.raft_log.synchronization.unsafe.disabled":                      {},
	"kv.range.backpressure_byte_tolerance":                             {},
	"kv.range_merge.queue_interval":                                    {},
	"kv.range_split.by_load_merge_delay":                               {},
	"kv.range_split.load_cpu_threshold":                                {},
	"kv.rangefeed.closed_timestamp_refresh_interval":                   {},
	"kv.rangefeed.closed_timestamp_smear_interval":                     {},
	"kv.rangefeed.range_stuck_threshold":                               {},
	"kv.replica_circuit_breaker.slow_replication_threshold":            {},
	"kv.replication_reports.interval":                                  {},
	"kv.snapshot.ingest_as_write_threshold":                            {},
	"kv.snapshot_rebalance.max_rate":                                   {},
	"kv.snapshot_sender.batch_size":                                    {},
	"kv.snapshot_sst.sync_size":                                        {},
	"kv.split.slow_split_tracing_threshold":                            {},
	"kv.trace.slow_request_stacks.threshold":                           {},
	"kv.trace.snapshot.enable_threshold":                               {},
	"kvadmission.elastic_cpu.duration_per_export_request":              {},
	"kvadmission.elastic_cpu.duration_per_low_pri_read":                {},
	"kvadmission.elastic_cpu.duration_per_rangefeed_scan_unit":         {},
	"kvadmission.flow_control.dispatch.max_bytes":                      {},
	"kvadmission.flow_controller.elastic_tokens_per_stream":            {},
	"kvadmission.flow_controller.regular_tokens_per_stream":            {},
	"kvadmission.flow_token.dispatch_interval":                         {},
	"kvadmission.flow_token.drop_interval":                             {},
	"kvadmission.raft_transport.connected_store_expiration":            {},
	"kvadmission.store.provisioned_bandwidth":                          {},
	"physical_replication.consumer.dump_frontier_entries_frequency":    {},
	"physical_replication.consumer.node_lag_replanning_threshold":      {},
	"physical_replication.consumer.timestamp_granularity":              {},
	"physical_replication.producer.timestamp_granularity":              {},
	"restore.frontier_checkpoint_max_bytes":                            {},
	"rocksdb.ingest_backpressure.max_delay":                            {},
	"rocksdb.min_wal_sync_interval":                                    {},
	"scheduler_latency.sample_duration":                                {},
	"scheduler_latency.sample_period":                                  {},
	"schemachanger.backfiller.buffer_size":                             {},
	"schemachanger.backfiller.max_buffer_size":                         {},
	"schemachanger.job.max_retry_backoff":                              {},
	"security.ocsp.timeout":                                            {},
	"server.active_query.total_dump_size_limit":                        {},
	"server.clock.persist_upper_bound_interval":                        {},
	"server.cockroach_cloud.max_client_connections_per_gateway_reason": {},
	"server.consistency_check.interval":                                {},
	"server.consistency_check.max_rate":                                {},
	"server.controller.mux_virtual_cluster_wait.timeout":               {},
	"server.cpu_profile.duration":                                      {},
	"server.cpu_profile.interval":                                      {},
	"server.cpu_profile.total_dump_size_limit":                         {},
	"server.debug.default_vmodule":                                     {},
	"server.eventlog.ttl":                                              {},
	"server.failed_reservation.timeout":                                {},
	"server.goroutine_dump.total_dump_size_limit":                      {},
	"server.host_based_authentication.configuration":                   {},
	"server.hot_ranges_request.node.timeout":                           {},
	"server.http.base_path":                                            {},
	"server.identity_map.configuration":                                {},
	"server.instance_id.reclaim_interval":                              {},
	"server.jemalloc.total_dump_size_limit":                            {},
	"server.jwt_authentication.audience":                               {},
	"server.jwt_authentication.claim":                                  {},
	"server.jwt_authentication.issuers":                                {},
	"server.jwt_authentication.jwks":                                   {},
	"server.log_gc.period":                                             {},
	"server.mem_monitoring.total_dump_size_limit":                      {},
	"server.mem_profile.total_dump_size_limit":                         {},
	"server.mem_stats.total_dump_size_limit":                           {},
	"server.oidc_authentication.button_text":                           {},
	"server.oidc_authentication.claim_json_key":                        {},
	"server.oidc_authentication.client_id":                             {},
	"server.oidc_authentication.client_secret":                         {},
	"server.oidc_authentication.principal_regex":                       {},
	"server.oidc_authentication.provider_url":                          {},
	"server.oidc_authentication.redirect_url":                          {},
	"server.rangelog.ttl":                                              {},
	"server.shutdown.connections.timeout":                              {},
	"server.shutdown.initial_wait":                                     {},
	"server.shutdown.jobs.timeout":                                     {},
	"server.shutdown.lease_transfer_iteration.timeout":                 {},
	"server.shutdown.transactions.timeout":                             {},
	"server.span_stats.node.timeout":                                   {},
	"server.sql_tcp_keep_alive.interval":                               {},
	"server.sqlliveness.gc_interval":                                   {},
	"server.sqlliveness.heartbeat":                                     {},
	"server.sqlliveness.ttl":                                           {},
	"server.telemetry.hot_ranges_stats.interval":                       {},
	"server.telemetry.hot_ranges_stats.logging_delay":                  {},
	"server.time_after_store_suspect":                                  {},
	"server.time_until_store_dead":                                     {},
	"server.user_login.password_encryption":                            {},
	"server.user_login.timeout":                                        {},
	"server.web_session.purge.ttl":                                     {},
	"server.web_session.timeout":                                       {},
	"spanconfig.kvsubscriber.metrics_poller_interval":                  {},
	"spanconfig.reconciliation_job.check_interval":                     {},
	"spanconfig.reconciliation_job.checkpoint_interval":                {},
	"spanconfig.store.fallback_config_override":                        {},
	"sql.catalog.descriptor_lease_duration":                            {},
	"sql.catalog.descriptor_lease_renewal_fraction":                    {},
	"sql.conn.max_read_buffer_message_size":                            {},
	"sql.contention.event_store.capacity":                              {},
	"sql.contention.event_store.duration_threshold":                    {},
	"sql.contention.event_store.resolution_interval":                   {},
	"sql.contention.txn_id_cache.max_size":                             {},
	"sql.crdb_internal.table_row_statistics.as_of_time":                {},
	"sql.create_virtual_cluster.default_template":                      {},
	"sql.defaults.datestyle":                                           {},
	"sql.defaults.experimental_computed_column_rewrites":               {},
	"sql.defaults.idle_in_session_timeout":                             {},
	"sql.defaults.idle_in_transaction_session_timeout":                 {},
	"sql.defaults.lock_timeout":                                        {},
	"sql.defaults.primary_region":                                      {},
	"sql.defaults.results_buffer.size":                                 {},
	"sql.defaults.statement_timeout":                                   {},
	"sql.distsql.flow_stream.timeout":                                  {},
	"sql.distsql.index_join_streamer.batch_size":                       {},
	"sql.distsql.join_reader_index_join_strategy.batch_size":           {},
	"sql.distsql.join_reader_no_ordering_strategy.batch_size":          {},
	"sql.distsql.join_reader_ordering_strategy.batch_size":             {},
	"sql.distsql.temp_storage.workmem":                                 {},
	"sql.gc_job.idle_wait_duration":                                    {},
	"sql.gc_job.wait_for_gc.interval":                                  {},
	"sql.guardrails.max_row_size_err":                                  {},
	"sql.guardrails.max_row_size_log":                                  {},
	"sql.history_retention_job.poll_interval":                          {},
	"sql.index_recommendation.drop_unused_duration":                    {},
	"sql.insights.anomaly_detection.latency_threshold":                 {},
	"sql.insights.anomaly_detection.memory_limit":                      {},
	"sql.insights.latency_threshold":                                   {},
	"sql.internal_executor.session_overrides":                          {},
	"sql.log.slow_query.latency_threshold":                             {},
	"sql.log.user_audit":                                               {},
	"sql.metrics.discarded_stats_log.interval":                         {},
	"sql.metrics.statement_details.plan_collection.period":             {},
	"sql.metrics.statement_details.threshold":                          {},
	"sql.mutations.mutation_batch_byte_size":                           {},
	"sql.region_liveness.probe.timeout":                                {},
	"sql.schema.force_declarative_statements":                          {},
	"sql.schema.telemetry.recurrence":                                  {},
	"sql.session_transfer.max_session_size":                            {},
	"sql.stats.aggregation.interval":                                   {},
	"sql.stats.cleanup.recurrence":                                     {},
	"sql.stats.flush.interval":                                         {},
	"sql.stats.flush.minimum_interval":                                 {},
	"sql.stats.garbage_collection_interval":                            {},
	"sql.stats.limit_table_size_check.interval":                        {},
	"sql.stats.max_timestamp_age":                                      {},
	"sql.stats.non_default_columns.min_retention_period":               {},
	"sql.stmt_diagnostics.bundle_chunk_size":                           {},
	"sql.stmt_diagnostics.in_flight_trace_collector.poll_interval":     {},
	"sql.stmt_diagnostics.poll_interval":                               {},
	"sql.telemetry.capture_index_usage_stats.check_enabled_interval":   {},
	"sql.telemetry.capture_index_usage_stats.interval":                 {},
	"sql.telemetry.capture_index_usage_stats.logging_delay":            {},
	"sql.temp_object_cleaner.cleanup_interval":                         {},
	"sql.temp_object_cleaner.wait_interval":                            {},
	"sql.trace.stmt.enable_threshold":                                  {},
	"sql.trace.txn.enable_threshold":                                   {},
	"sqladmission.elastic_cpu.duration_per_low_pri_read_response":      {},
	"storage.max_sync_duration":                                        {},
	"storage.wal_failover.unhealthy_op_threshold":                      {},
	"physical_replication.consumer.heartbeat_frequency":                {},
	"physical_replication.consumer.job_checkpoint_frequency":           {},
	"physical_replication.producer.min_checkpoint_frequency":           {},
	"physical_replication.consumer.replan_flow_frequency":              {},
	"physical_replication.producer.stream_liveness_track_frequency":    {},
	"tenant_cost_control.token_request_period":                         {},
	"tenant_cost_model.cross_region_network_cost":                      {},
	"tenant_cost_control.cpu_usage_allowance":                          {},
	"tenant_global_metrics_exporter_interval":                          {},
	"tenant_cost_control.instance_inactivity.timeout":                  {},
	"timeseries.storage.resolution_10s.ttl":                            {},
	"timeseries.storage.resolution_30m.ttl":                            {},
	"trace.opentelemetry.collector":                                    {},
	"trace.snapshot.rate":                                              {},
	"trace.span.force_verbose_regexp":                                  {},
	"trace.zipkin.collector":                                           {},
	"ui.display_timezone":                                              {},
}
