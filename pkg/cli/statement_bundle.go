// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	statementBundleRecreateCmd.RunE = clierrorplus.MaybeDecorateError(runBundleRecreate)

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
	ret.env, err = os.ReadFile(filepath.Join(zipdir, "env.sql"))
	if err != nil {
		return ret, err
	}
	ret.schema, err = os.ReadFile(filepath.Join(zipdir, "schema.sql"))
	if err != nil {
		return ret, err
	}
	ret.statement, err = os.ReadFile(filepath.Join(zipdir, "statement.sql"))
	if err != nil {
		// In 21.2 and prior releases, the statement file had 'txt' extension,
		// let's try that.
		var newErr error
		ret.statement, newErr = os.ReadFile(filepath.Join(zipdir, "statement.txt"))
		if newErr != nil {
			return ret, errors.CombineErrors(err, newErr)
		}
	}

	return ret, filepath.WalkDir(zipdir, func(path string, d fs.DirEntry, _ error) error {
		if d.IsDir() {
			return nil
		}
		if !strings.HasPrefix(d.Name(), "stats-") {
			return nil
		}
		f, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		ret.stats = append(ret.stats, f)
		return nil
	})
}

func runBundleRecreate(cmd *cobra.Command, args []string) (resErr error) {
	zipdir := args[0]
	bundle, err := loadStatementBundle(zipdir)
	if err != nil {
		return err
	}

	var demoLocalityInfo string
	schema := string(bundle.schema)
	if strings.Contains(schema, "REGIONS = ") {
		// We have at least one multi-region DB, so we'll extract all regions.
		regions := make(map[string]struct{})
		for _, line := range strings.Split(schema, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "CREATE DATABASE") {
				stmt, err := parser.ParseOne(line)
				if err != nil {
					return err
				}
				createDB, ok := stmt.AST.(*tree.CreateDatabase)
				if !ok {
					return errors.Errorf("expected *tree.CreateDatabase AST node but got %T for %s", stmt.AST, line)
				}
				for _, region := range createDB.Regions.ToStrings() {
					if _, ok = regions[region]; !ok {
						regions[region] = struct{}{}
					}
				}
			}
		}
		if len(regions) > 0 {
			// Every region will get exactly one node.
			demoCtx.NumNodes = len(regions)
			var regionsString string
			for region := range regions {
				demoCtx.Localities = append(demoCtx.Localities, roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: region},
						{Key: "az", Value: "1"}, // this shouldn't matter
					},
				})
				if regionsString != "" {
					regionsString += `, "` + region + `"`
				} else {
					regionsString += `"` + region + `"`
				}
			}
			demoLocalityInfo = fmt.Sprintf("\n# Started %d nodes with regions %s.", len(regions), regionsString)
		}
	}

	demoCtx.UseEmptyDatabase = true
	demoCtx.Multitenant = false
	return runDemoInternal(cmd, nil /* gen */, func(ctx context.Context, conn clisqlclient.Conn) error {
		// SET CLUSTER SETTING statements cannot be executed in multi-statement
		// implicit transaction, so we need to separate them out into their own
		// implicit transactions.
		initStmts := strings.Split(string(bundle.env), "SET CLUSTER SETTING")
		for i := 1; i < len(initStmts); i++ {
			initStmts[i] = "SET CLUSTER SETTING " + initStmts[i]
		}
		// All stmts before the first SET CLUSTER SETTING are SET stmts. We need
		// to handle 'SET database = ' stmt separately if found - the target
		// database might not exist yet.
		setStmts := strings.Split(initStmts[0], "\n")
		initStmts = initStmts[1:]
		var setDBStmt string
		for i, stmt := range setStmts {
			stmt = strings.TrimSpace(stmt)
			if strings.HasPrefix(stmt, "SET database = ") {
				setDBStmt = stmt
				setStmts = append(setStmts[:i], setStmts[i+1:]...)
				break
			}
		}
		initStmts = append(initStmts, setStmts...)
		// Disable auto stats collection (which would override the injected
		// stats).
		initStmts = append(initStmts, "SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;")
		initStmts = append(initStmts, string(bundle.schema))
		if setDBStmt != "" {
			initStmts = append(initStmts, setDBStmt)
		}
		for _, stats := range bundle.stats {
			initStmts = append(initStmts, string(stats))
		}
		for _, s := range initStmts {
			if err := conn.Exec(ctx, s); err != nil {
				return errors.Wrapf(err, "failed to run: %s", s)
			}
		}

		stmt, numPlaceholders, err := sql.ReplacePlaceholdersWithValuesForBundle(string(bundle.statement))
		if err != nil {
			return errors.Wrap(err, "failed to replace placeholders")
		}
		var placeholderInfo string
		if numPlaceholders > 0 {
			var plural string
			if numPlaceholders > 1 {
				plural = "s"
			}
			placeholderInfo = fmt.Sprintf("(had %d placeholder%s) ", numPlaceholders, plural)
		}

		cliCtx.PrintfUnlessEmbedded(`#
# Statement bundle %s loaded.%s
# Autostats disabled.
#
# Statement %swas:
#
# %s
`, zipdir, demoLocalityInfo, placeholderInfo, stmt+";")

		if placeholderPairs != nil {
			placeholderToColMap := make(map[int]string)
			placeholderFQColNames := make(map[string]struct{})
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
				placeholderFQColNames[pair[1]] = struct{}{}
			}
			inputs, outputs, err := getExplainCombinations(
				ctx, conn, explainPrefix, placeholderToColMap, placeholderFQColNames, bundle,
			)
			if err != nil {
				return err
			}

			cliCtx.PrintfUnlessEmbedded("found %d unique explains:\n\n", len(inputs))
			for i, inputs := range inputs {
				cliCtx.PrintfUnlessEmbedded("Values %s: \n%s\n----\n\n", inputs, outputs[i])
			}
		}

		return nil
	})
}

// placeholderRe matches the placeholder format at the bottom of statement.txt
// in a statement bundle. It looks like this:
//
// $1: blah
// $2: 1
var placeholderRe = regexp.MustCompile(`\$(\d+): .*`)

// The double quotes are needed for table names that are reserved keywords.
var statsRe = regexp.MustCompile(`ALTER TABLE ([\w".]+) INJECT STATISTICS '`)

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
	ctx context.Context,
	conn clisqlclient.Conn,
	explainPrefix string,
	placeholderToColMap map[int]string,
	placeholderFQColNames map[string]struct{},
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
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

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
			if _, isPlaceholder := placeholderFQColNames[fqColName]; !isPlaceholder {
				// This column is not one of the placeholder values, so simply
				// ignore it.
				continue
			}
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
				// Empty 'histo_col_type' is used when there is no histogram for
				// the column, simply skip this stat (see stats/json.go for more
				// details).
				continue
			}
			colTypeRef, err := parser.GetTypeFromValidSQLSyntax(typ)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "unable to parse type %s for col %s", typ, col)
			}
			colType := tree.MustBeStaticallyKnownType(colTypeRef)
			if stat["histo_buckets"] == nil {
				// There might not be any histogram buckets if the stats were
				// collected when the table was empty or all values in the
				// column were NULL.
				continue
			}
			buckets := stat["histo_buckets"].([]interface{})
			// addedNonExistent tracks whether we included at least one
			// "previous" datum which - according to the histograms - is not
			// present in the table.
			var addedNonExistent bool
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
				datum, err := rowenc.ParseDatumStringAs(ctx, colType, upperBound, &evalCtx, nil /* semaCtx */)
				if err != nil {
					panic("failed parsing datum string as " + datum.String() + " " + err.Error())
				}
				if maxUpperBound == nil {
					maxUpperBound = datum
				} else if cmp, err := maxUpperBound.Compare(ctx, &evalCtx, datum); err != nil {
					panic(err)
				} else if cmp < 0 {
					maxUpperBound = datum
				}
				// If we have any datums within the bucket (i.e. not equal to
				// the upper bound), we always attempt to add a "previous" to
				// the upper bound datum.
				addPrevious := numRange > 0
				if numRange == 0 && !addedNonExistent {
					// If our bucket says that there are no values present in
					// the table between the current upper bound and the upper
					// bound of the previous histogram bucket, then we only
					// attempt to add the "previous" non-existent datum if we
					// haven't done so already (this is to avoid the redundant
					// non-existent values which would get treated in the same
					// fashion anyway).
					addPrevious = true
				}
				if addPrevious {
					if prev, ok := tree.DatumPrev(ctx, datum, &evalCtx, &evalCtx.CollationEnv); ok {
						bucketMap[key] = append(bucketMap[key], tree.AsStringWithFlags(prev, fmtCtx))
						addedNonExistent = addedNonExistent || numRange == 0
					}
				}
			}
			colSamples := make([]string, 0, len(bucketMap))
			for _, samples := range bucketMap {
				colSamples = append(colSamples, samples...)
			}
			// Create a value that's outside of histogram range by incrementing the
			// max value that we've seen.
			if outside, ok := tree.DatumNext(ctx, maxUpperBound, &evalCtx, &evalCtx.CollationEnv); ok {
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
	fmt.Printf("trying %d placeholder combinations\n", len(inputs))
	for i, values := range inputs {
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
		if (i+1)%1000 == 0 {
			fmt.Printf("%d placeholder combinations are done\n", i+1)
		}
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
