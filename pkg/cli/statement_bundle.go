// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugStatementBundleCmd = &cobra.Command{
	Use:     "statement-bundle [command]",
	Aliases: []string{"sb"},
	Short:   "run a cockroach debug statement-bundle tool command",
	Long: `
debug statement-bundle is a suite of tools for debugging and manipulating statement
bundles created with EXPLAIN ANALYZE (DEBUG).
`,
}

var statementBundleRecreateCmd = &cobra.Command{
	Use:   "recreate <stmt bundle zipdir>",
	Short: "recreate the statement bundle in a demo cluster",
	Long: `
Run the recreate tool to populate a demo cluster with the environment, schema,
and stats in an unzipped statement bundle directory.
`,
	Args: cobra.ExactArgs(1),
}

var placeholderPairs []string
var explainPrefix string

func init() {
	statementBundleRecreateCmd.RunE = clierrorplus.MaybeDecorateError(func(cmd *cobra.Command, args []string) error {
		return runBundleRecreate(cmd, args)
	})

	statementBundleRecreateCmd.Flags().StringArrayVar(&placeholderPairs, "placeholder", nil,
		"pass in a map of placeholder id to fully-qualified table column to get the program to produce all optimal"+
			" of explain plans with each of the histogram values for each column replaced in its placeholder.")
	statementBundleRecreateCmd.Flags().StringVar(&explainPrefix, "explain-cmd", "EXPLAIN",
		"set the EXPLAIN command used to produce the final output when displaying all optimal explain plans with"+
			" --placeholder. Example: EXPLAIN(OPT)")
}

type statementBundle struct {
	env       []byte
	schema    []byte
	statement []byte
	stats     [][]byte
}

func loadStatementBundle(zipdir string) (*statementBundle, error) {
	ret := &statementBundle{}
	var err error
	ret.env, err = ioutil.ReadFile(filepath.Join(zipdir, "env.sql"))
	if err != nil {
		return ret, err
	}
	ret.schema, err = ioutil.ReadFile(filepath.Join(zipdir, "schema.sql"))
	if err != nil {
		return ret, err
	}
	ret.statement, err = ioutil.ReadFile(filepath.Join(zipdir, "statement.txt"))
	if err != nil {
		return ret, err
	}

	return ret, filepath.WalkDir(zipdir, func(path string, d fs.DirEntry, _ error) error {
		if d.IsDir() {
			return nil
		}
		if !strings.HasPrefix(d.Name(), "stats-") {
			return nil
		}
		f, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		ret.stats = append(ret.stats, f)
		return nil
	})
}

func runBundleRecreate(cmd *cobra.Command, args []string) error {
	zipdir := args[0]
	bundle, err := loadStatementBundle(zipdir)
	if err != nil {
		return err
	}

	closeFn, err := sqlCtx.Open(os.Stdin)
	if err != nil {
		return err
	}
	defer closeFn()
	ctx := context.Background()
	c, err := democluster.NewDemoCluster(ctx, &demoCtx,
		log.Infof,
		log.Warningf,
		log.Ops.Shoutf,
		func(ctx context.Context) (*stop.Stopper, error) {
			// Override the default server store spec.
			//
			// This is needed because the logging setup code peeks into this to
			// decide how to enable logging.
			serverCfg.Stores.Specs = nil
			return setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
		},
		getAdminClient,
		func(ctx context.Context, ac serverpb.AdminClient) error {
			return drainAndShutdown(ctx, ac, "local" /* targetNode */)
		},
	)
	if err != nil {
		c.Close(ctx)
		return err
	}
	defer c.Close(ctx)

	initGEOS(ctx)

	if err := c.Start(ctx, runInitialSQL); err != nil {
		return clierrorplus.CheckAndMaybeShout(err)
	}
	conn, err := sqlCtx.MakeConn(c.GetConnURL())
	if err != nil {
		return err
	}
	// Disable autostats collection, which will override the injected stats.
	if err := conn.Exec(ctx,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`); err != nil {
		return err
	}
	var initStmts = [][]byte{bundle.env, bundle.schema}
	initStmts = append(initStmts, bundle.stats...)
	for _, a := range initStmts {
		if err := conn.Exec(ctx, string(a)); err != nil {
			return errors.Wrapf(err, "failed to run %s", a)
		}
	}

	cliCtx.PrintfUnlessEmbedded(`#
# Statement bundle %s loaded.
# Autostats disabled.
#
# Statement was:
#
# %s
`, zipdir, bundle.statement)

	if placeholderPairs != nil {
		placeholderToColMap := make(map[int]string)
		for _, placeholderPairStr := range placeholderPairs {
			pair := strings.Split(placeholderPairStr, "=")
			if len(pair) != 2 {
				return errors.New("use --placeholder='1=schema.table.col' --placeholder='2=schema.table.col...'")
			}
			n, err := strconv.Atoi(pair[0])
			if err != nil {
				return err
			}
			placeholderToColMap[n] = pair[1]
		}
		inputs, outputs, err := getExplainCombinations(conn, explainPrefix, placeholderToColMap, bundle)
		if err != nil {
			return err
		}

		cliCtx.PrintfUnlessEmbedded("found %d unique explains:\n\n", len(inputs))
		for i, inputs := range inputs {
			cliCtx.PrintfUnlessEmbedded("Values %s: \n%s\n----\n\n", inputs, outputs[i])
		}
	}

	sqlCtx.ShellCtx.DemoCluster = c
	return sqlCtx.Run(conn)
}

// placeholderRe matches the placeholder format at the bottom of statement.txt
// in a statement bundle. It looks like this:
//
// $1: blah
// $2: 1
var placeholderRe = regexp.MustCompile(`\$(\d+): .*`)

var statsRe = regexp.MustCompile(`ALTER TABLE ([\w.]+) INJECT STATISTICS '`)

type bucketKey struct {
	NumEq         float64
	NumRange      float64
	DistinctRange float64
}

// getExplainCombinations finds all unique optimal explain plans for a given statement
// bundle that are produced by creating all combinations of plans where each
// placeholder is replaced by every value in the column histogram for a linked
// column.
//
// explainPrefix is the type of EXPLAIN to use for the final output, like
// EXPLAIN(OPT).
//
// A list of unique inputs is returned, which corresponds 1 to 1 with the list
// of explain outputs: the ith set of inputs is the set of placeholders that
// produced the ith explain output.
//
// Columns are linked to placeholders by the --placeholder=n=schema.table.col
// commandline flags.
func getExplainCombinations(
	conn clisqlclient.Conn,
	explainPrefix string,
	placeholderToColMap map[int]string,
	bundle *statementBundle,
) (inputs [][]string, explainOutputs []string, err error) {

	stmtComponents := strings.Split(string(bundle.statement), "Arguments:")
	statement := strings.TrimSpace(stmtComponents[0])
	placeholders := stmtComponents[1]

	var stmtPlaceholders []int
	for _, line := range strings.Split(placeholders, "\n") {
		// The placeholderRe has 1 matching group, so the length of the matches
		// list will be 2 if we see a successful match.
		if matches := placeholderRe.FindStringSubmatch(line); len(matches) == 2 {
			// The first matching group is the number of the placeholder. Extract it
			// into an integer.
			n, err := strconv.Atoi(matches[1])
			if err != nil {
				return nil, nil, err
			}
			stmtPlaceholders = append(stmtPlaceholders, n)
		}
	}

	for _, n := range stmtPlaceholders {
		if placeholderToColMap[n] == "" {
			return nil, nil, errors.Errorf("specify --placeholder= for placeholder %d", n)
		}
	}
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	fmtCtx := tree.FmtBareStrings

	// Map from fully-qualified column name to list of histogram upper_bound
	// values with unique bucket attributes.
	statsMap := make(map[string][]string)
	statsAge := make(map[string]time.Time)
	for _, statsBytes := range bundle.stats {
		statsStr := string(statsBytes)
		matches := statsRe.FindStringSubmatch(statsStr)
		if len(matches) != 2 {
			return nil, nil, errors.Errorf("invalid stats file %s", statsStr)
		}
		tableName := matches[1]
		// Find the first instance of ', which is the beginning of the JSON payload.
		idx := bytes.IndexByte(statsBytes, '\'')
		var statsJSON []map[string]interface{}

		// Snip off the last 3 characters, which are ';\n, the end of the JSON payload.
		data := statsBytes[idx+1 : len(statsBytes)-3]
		if err := json.Unmarshal(data, &statsJSON); err != nil {
			return nil, nil, err
		}

		// Map a bucket key (a bucket without upper bound) to an upper bound sample
		// and its Prev value (for non-0 width buckets).
		// This deduplicates identical buckets.
		for _, stat := range statsJSON {
			bucketMap := make(map[bucketKey][]string)
			columns := stat["columns"].([]interface{})
			if len(columns) > 1 {
				// Ignore multi-col stats.
				continue
			}
			col := columns[0]
			fqColName := fmt.Sprintf("%s.%s", tableName, col)
			d, _, err := tree.ParseDTimestamp(nil, stat["created_at"].(string), time.Microsecond)
			if err != nil {
				panic(err)
			}
			if lastStat, ok := statsAge[fqColName]; ok && d.Before(lastStat) {
				// Skip stats that are older than the most recent stat.
				continue
			}
			statsAge[fqColName] = d.Time

			typ := stat["histo_col_type"].(string)
			if typ == "" {
				fmt.Println("Ignoring column with empty type ", col)
				continue
			}
			colTypeRef, err := parser.GetTypeFromValidSQLSyntax(typ)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "unable to parse type %s for col %s", typ, col)
			}
			colType := tree.MustBeStaticallyKnownType(colTypeRef)
			buckets := stat["histo_buckets"].([]interface{})
			var maxUpperBound tree.Datum
			for _, b := range buckets {
				bucket := b.(map[string]interface{})
				numRange := bucket["num_range"].(float64)
				key := bucketKey{
					NumEq:         bucket["num_eq"].(float64),
					NumRange:      numRange,
					DistinctRange: bucket["distinct_range"].(float64),
				}
				upperBound := bucket["upper_bound"].(string)
				bucketMap[key] = []string{upperBound}
				datum, err := rowenc.ParseDatumStringAs(colType, upperBound, &evalCtx)
				if err != nil {
					panic("failed parsing datum string as " + datum.String() + " " + err.Error())
				}
				if maxUpperBound == nil || maxUpperBound.Compare(&evalCtx, datum) < 0 {
					maxUpperBound = datum
				}
				if numRange > 0 {
					if prev, ok := datum.Prev(&evalCtx); ok {
						bucketMap[key] = append(bucketMap[key], tree.AsStringWithFlags(prev, fmtCtx))
					}
				}
			}
			colSamples := make([]string, 0, len(bucketMap))
			for _, samples := range bucketMap {
				colSamples = append(colSamples, samples...)
			}
			// Create a value that's outside of histogram range by incrementing the
			// max value that we've seen.
			if outside, ok := maxUpperBound.Next(&evalCtx); ok {
				colSamples = append(colSamples, tree.AsStringWithFlags(outside, fmtCtx))
			}
			sort.Strings(colSamples)
			statsMap[fqColName] = colSamples
		}
	}

	for _, fqColName := range placeholderToColMap {
		if statsMap[fqColName] == nil {
			return nil, nil, errors.Errorf("no stats found for %s", fqColName)
		}
	}

	combinations := getPlaceholderCombinations(stmtPlaceholders, placeholderToColMap, statsMap)

	outputs, err := getExplainOutputs(conn, "EXPLAIN(SHAPE)", statement, combinations)
	if err != nil {
		return nil, nil, err
	}
	// uniqueExplains maps explain output to the list of placeholders that
	// produced it.
	uniqueExplains := make(map[string][]string)
	for i := range combinations {
		uniqueExplains[outputs[i]] = combinations[i]
	}

	// Sort the explain outputs for consistent results.
	explains := make([]string, 0, len(uniqueExplains))
	for key := range uniqueExplains {
		explains = append(explains, key)
	}
	sort.Strings(explains)

	// Now that we've got the unique explain shapes, re-run them with the desired
	// EXPLAIN style to get sufficient detail.
	uniqueInputs := make([][]string, 0, len(uniqueExplains))
	for _, explain := range explains {
		input := uniqueExplains[explain]
		uniqueInputs = append(uniqueInputs, input)
	}
	outputs, err = getExplainOutputs(conn, explainPrefix, statement, uniqueInputs)
	if err != nil {
		return nil, nil, err
	}

	return uniqueInputs, outputs, nil
}

// getExplainOutputs runs the explain style given in explainPrefix on the
// statement once for every input (an ordered list of placeholder values) in the
// input list. The result is returned in a list of explain outputs, where the
// ith explain output was generated from the ith input.
func getExplainOutputs(
	conn clisqlclient.Conn, explainPrefix string, statement string, inputs [][]string,
) (explainStrings []string, err error) {
	for _, values := range inputs {
		// Run an explain for each possible input.
		query := fmt.Sprintf("%s %s", explainPrefix, statement)
		args := make([]interface{}, len(values))
		for i, s := range values {
			args[i] = s
		}
		rows, err := conn.Query(context.Background(), query, args...)
		if err != nil {
			return nil, err
		}
		row := []driver.Value{""}
		var explainStr = strings.Builder{}
		for err = rows.Next(row); err == nil; err = rows.Next(row) {
			fmt.Fprintln(&explainStr, row[0])
		}
		if err != io.EOF {
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		explainStrings = append(explainStrings, explainStr.String())
	}
	return explainStrings, nil
}

// getPlaceholderCombinations returns a list of lists, which each inner list is
// a possible set of placeholders that can be inserted into the statement, where
// each possible value for each placeholder is taken from the input statsMap.
func getPlaceholderCombinations(
	remainingPlaceholders []int, placeholderMap map[int]string, statsMap map[string][]string,
) [][]string {
	placeholder := remainingPlaceholders[0]
	fqColName := placeholderMap[placeholder]
	var rest = [][]string{nil}
	if len(remainingPlaceholders) > 1 {
		// Recurse to get the rest of the combinations.
		rest = getPlaceholderCombinations(remainingPlaceholders[1:], placeholderMap, statsMap)
	}
	var ret [][]string
	for _, val := range statsMap[fqColName] {
		for _, inner := range rest {
			ret = append(ret, append([]string{val}, inner...))
		}
	}
	return ret
}
