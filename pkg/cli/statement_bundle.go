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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/democluster"
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

func init() {
	statementBundleRecreateCmd.RunE = MaybeDecorateGRPCError(func(cmd *cobra.Command, args []string) error {
		return runBundleRecreate(cmd, args)
	})

	statementBundleRecreateCmd.Flags().StringArrayVar(&placeholderPairs, "placeholder", nil,
		"pass in a map of placeholder id to fully-qualified table column to get the program to produce all combinations"+
			" of explain plans with each of the histogram values for each column replaced in its placeholder.")
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
		drainAndShutdown,
	)
	if err != nil {
		c.Close(ctx)
		return err
	}
	defer c.Close(ctx)

	initGEOS(ctx)

	if err := c.Start(ctx, runInitialSQL); err != nil {
		return CheckAndMaybeShout(err)
	}
	conn, err := sqlCtx.MakeConn(c.GetConnURL())
	if err != nil {
		return err
	}
	// Disable autostats collection, which will override the injected stats.
	if err := conn.Exec(`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`, nil); err != nil {
		return err
	}
	var initStmts = [][]byte{bundle.env, bundle.schema}
	initStmts = append(initStmts, bundle.stats...)
	for _, a := range initStmts {
		if err := conn.Exec(string(a), nil); err != nil {
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
		if err := runExplainCombinations(cliCtx, conn, bundle); err != nil {
			return err
		}
	}

	sqlCtx.ShellCtx.DemoCluster = c
	return sqlCtx.Run(conn)
}

var placeholderRe = regexp.MustCompile(`(\d+): (.*)`)

var statsRe = regexp.MustCompile(`ALTER TABLE ([\w.]+) INJECT STATISTICS '`)

func runExplainCombinations(ctx cliContext, conn clisqlclient.Conn, bundle *statementBundle) error {
	placeholderMap := make(map[int]string)
	for _, placeholderPairStr := range placeholderPairs {
		fmt.Println(placeholderPairStr)
		pair := strings.Split(placeholderPairStr, "=")
		if len(pair) != 2 {
			return errors.New("use --placeholder='1=schema.table.col 2=schema.table.col...'")
		}
		n, err := strconv.Atoi(pair[0])
		if err != nil {
			return err
		}
		placeholderMap[n] = pair[1]
	}

	stmtComponents := strings.Split(string(bundle.statement), "Arguments:")
	statement := strings.TrimSpace(stmtComponents[0])
	placeholders := stmtComponents[1]

	var stmtPlaceholders []int
	for _, line := range strings.Split(placeholders, "\n") {
		if matches := placeholderRe.FindStringSubmatch(line); len(matches) == 3 {
			n, err := strconv.Atoi(matches[1])
			if err != nil {
				return err
			}
			stmtPlaceholders = append(stmtPlaceholders, n)
		}
	}

	for _, n := range stmtPlaceholders {
		if placeholderMap[n] == "" {
			return errors.Errorf("specify --placeholder= for placeholder %d", n)
		}
	}

	// map from fully-qualified column name to list of histogram upper_bound
	// values with unique bucket attributes.
	statsMap := make(map[string][]string)
	for _, statsBytes := range bundle.stats {
		statsStr := string(statsBytes)
		matches := statsRe.FindStringSubmatch(statsStr)
		if len(matches) != 2 {
			return errors.Errorf("invalid stats file %s", statsStr)
		}
		tableName := matches[1]
		// Find the first instance of ', which is the beginning of the JSON payload.
		idx := bytes.IndexByte(statsBytes, '\'')
		var statsJSON []map[string]interface{}

		// Snip off the last 3 characters, which are ';\n, the end of the JSON payload.
		data := statsBytes[idx+1 : len(statsBytes)-3]
		if err := json.Unmarshal(data, &statsJSON); err != nil {
			return err
		}

		// map a bucket key (a bucket without upper bound) to an upper bound sample.
		// This deduplicates identical buckets.
		for _, stat := range statsJSON {
			bucketMap := make(map[bucketKey]string)
			columns := stat["columns"].([]interface{})
			if len(columns) > 1 {
				// Ignore multi-col stats.
				continue
			}
			col := columns[0]

			buckets := stat["histo_buckets"].([]interface{})
			for _, b := range buckets {
				bucket := b.(map[string]interface{})
				key := bucketKey{
					NumEq:         int64(bucket["num_eq"].(float64)),
					NumRange:      int64(bucket["num_range"].(float64)),
					DistinctRange: bucket["distinct_range"].(float64),
				}
				bucketMap[key] = bucket["upper_bound"].(string)
			}
			upperBounds := make([]string, 0, len(bucketMap))
			for _, upperBound := range bucketMap {
				upperBounds = append(upperBounds, upperBound)
			}
			fqColName := fmt.Sprintf("%s.%s", tableName, col)
			statsMap[fqColName] = upperBounds
		}
	}

	combinations := getPlaceholderCombinations(stmtPlaceholders, placeholderMap, statsMap)

	// uniqueExplains maps explain output to the list of placeholders that
	// produced it.
	uniqueExplains := make(map[string][]string)
	for _, values := range combinations {
		// Run an explain for each possible combination.
		stmt := statement
		for i, n := range stmtPlaceholders {
			stmt = strings.ReplaceAll(stmt, fmt.Sprintf("$%d", n), values[i])
		}

		dvals := make([]driver.Value, len(values))
		for i := range values {
			dvals[i] = values[i]
		}

		query := "EXPLAIN(SHAPE) " + statement
		cliCtx.PrintfUnlessEmbedded("> %s (%s)\n", query, dvals)
		rows, err := conn.Query(query, dvals)
		if err != nil {
			return err
		}
		row := []driver.Value{""}
		var explainStr = strings.Builder{}
		for err = rows.Next(row); err == nil; err = rows.Next(row) {
			fmt.Fprintln(&explainStr, row[0])
		}
		if err != io.EOF {
			return err
		}
		rows.Close()

		uniqueExplains[explainStr.String()] = values
	}

	cliCtx.PrintfUnlessEmbedded("found %d unique explains:\n\n", len(uniqueExplains))
	for explain, vals := range uniqueExplains {
		cliCtx.PrintfUnlessEmbedded("Values %s: \n%s\n----\n\n", vals, explain)
	}

	return nil
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

type bucketKey struct {
	NumEq         int64
	NumRange      int64
	DistinctRange float64
}
