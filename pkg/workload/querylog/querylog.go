// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package querylog

import (
	"archive/zip"
	"bufio"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	workloadrand "github.com/cockroachdb/cockroach/pkg/workload/rand"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx"
	"github.com/lib/pq/oid"
	"github.com/spf13/pflag"
)

type querylog struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	state querylogState

	dirPath            string
	zipPath            string
	filesToParse       int
	minSamplingProb    float64
	nullPct            int
	numSamples         int
	omitDeleteQueries  bool
	omitWriteQueries   bool
	probOfUsingSamples float64
	// querybenchPath (if set) tells the querylog where to write the generated
	// queries to be later executed by querybench in round-robin fashion, i.e.
	// querylog itself won't execute the queries against the database and only
	// will prepare the file.
	querybenchPath string
	count          uint
	seed           int64
	// TODO(yuzefovich): this is a great variable to move to the main generator.
	stmtTimeoutSeconds int64
	verbose            bool
}

type querylogState struct {
	tableNames             []string
	totalQueryCount        int
	queryCountPerTable     []int
	seenQueriesByTableName []map[string]int
	tableUsed              map[string]bool
	columnsByTableName     map[string][]columnInfo
}

func init() {
	workload.Register(querylogMeta)
}

var querylogMeta = workload.Meta{
	Name:        `querylog`,
	Description: `Querylog is a tool that produces a workload based on the provided query log.`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &querylog{}
		g.flags.FlagSet = pflag.NewFlagSet(`querylog`, pflag.ContinueOnError)
		g.flags.Meta = map[string]workload.FlagMeta{
			`dir`:                   {RuntimeOnly: true},
			`files-to-parse`:        {RuntimeOnly: true},
			`min-sampling-prob`:     {RuntimeOnly: true},
			`null-percent`:          {RuntimeOnly: true},
			`num-samples`:           {RuntimeOnly: true},
			`omit-delete-queries`:   {RuntimeOnly: true},
			`omit-write-queries`:    {RuntimeOnly: true},
			`prob-of-using-samples`: {RuntimeOnly: true},
			`seed`:                  {RuntimeOnly: true},
			`statement-timeout`:     {RuntimeOnly: true},
			`verbose`:               {RuntimeOnly: true},
			`zip`:                   {RuntimeOnly: true},
		}
		g.flags.UintVar(&g.count, `count`, 100, `Number of queries to be written for querybench (used only if --querybench-path is specified).`)
		g.flags.StringVar(&g.dirPath, `dir`, ``, `Directory of the querylog files.`)
		g.flags.IntVar(&g.filesToParse, `files-to-parse`, 5, `Maximum number of files in the query log to process.`)
		g.flags.Float64Var(&g.minSamplingProb, `min-sampling-prob`, 0.01, `Minimum sampling probability defines the minimum chance `+
			`that a value will be chosen as a sample. The smaller the number is the more diverse samples will be (given that the table has enough values). `+
			`However, at the same time, the sampling process will be slower.`)
		g.flags.IntVar(&g.nullPct, `null-percent`, 5, `Percent random nulls.`)
		g.flags.IntVar(&g.numSamples, `num-samples`, 1000, `Number of samples to be taken from the tables. The bigger this number, `+
			`the more diverse values will be chosen for the generated queries.`)
		g.flags.BoolVar(&g.omitDeleteQueries, `omit-delete-queries`, true, `Indicates whether delete queries should be omitted.`)
		g.flags.BoolVar(&g.omitWriteQueries, `omit-write-queries`, true, `Indicates whether write queries (INSERTs and UPSERTs) should be omitted.`)
		g.flags.Float64Var(&g.probOfUsingSamples, `prob-of-using-samples`, 0.9, `Probability of using samples to generate values for `+
			`the placeholders. Say it is 0.9, then with 0.1 probability the values will be generated randomly.`)
		// TODO(yuzefovich): improve termination of querylog when used for
		//  querybench file generation.
		g.flags.StringVar(&g.querybenchPath, `querybench-path`, ``, `Path to write the generated queries to for querybench tool. `+
			`NOTE: at the moment --max-ops=1 is the best way to terminate the generator to produce the desired count of queries.`)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Random number generator seed.`)
		g.flags.Int64Var(&g.stmtTimeoutSeconds, `statement-timeout`, 0, `Sets session's statement_timeout setting (in seconds).'`)
		g.flags.BoolVar(&g.verbose, `verbose`, false, `Indicates whether error messages should be printed out.`)
		g.flags.StringVar(&g.zipPath, `zip`, ``, `Path to the zip with the query log. Note: this zip will be extracted into a temporary `+
			`directory at the same path as zip just without '.zip' extension which will be removed after parsing is complete.`)

		g.connFlags = workload.NewConnFlags(&g.flags)
		return g
	},
}

// Meta implements the Generator interface.
func (*querylog) Meta() workload.Meta { return querylogMeta }

// Flags implements the Flagser interface.
func (w *querylog) Flags() workload.Flags { return w.flags }

// Tables implements the Generator interface.
func (*querylog) Tables() []workload.Table {
	// Assume the necessary tables are already present.
	return []workload.Table{}
}

// Hooks implements the Hookser interface.
func (w *querylog) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.zipPath == "" && w.dirPath == "" {
				return errors.Errorf("Missing required argument: either `--zip` or `--dir` have to be specified.")
			}
			if w.zipPath != "" {
				if w.zipPath[len(w.zipPath)-4:] != ".zip" {
					return errors.Errorf("Illegal argument: `--zip` is expected to end with '.zip'.")
				}
			}
			if w.minSamplingProb < 0.00000001 || w.minSamplingProb > 1.0 {
				return errors.Errorf("Illegal argument: `--min-sampling-prob` must be in [0.00000001, 1.0] range.")
			}
			if w.nullPct < 0 || w.nullPct > 100 {
				return errors.Errorf("Illegal argument: `--null-pct` must be in [0, 100] range.")
			}
			if w.probOfUsingSamples < 0 || w.probOfUsingSamples > 1.0 {
				return errors.Errorf("Illegal argument: `--prob-of-using-samples` must be in [0.0, 1.0] range.")
			}
			if w.stmtTimeoutSeconds < 0 {
				return errors.Errorf("Illegal argument: `--statement-timeout` must be a non-negative integer.")
			}
			return nil
		},
	}
}

// Ops implements the Opser interface.
func (w *querylog) Ops(
	ctx context.Context, urls []string, reg *histogram.Registry,
) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	if err = w.getTableNames(db); err != nil {
		return workload.QueryLoad{}, err
	}

	if err = w.processQueryLog(ctx); err != nil {
		return workload.QueryLoad{}, err
	}

	if err = w.getColumnsInfo(db); err != nil {
		return workload.QueryLoad{}, err
	}

	if err = w.populateSamples(ctx, db); err != nil {
		return workload.QueryLoad{}, err
	}

	// TODO(yuzefovich): implement round-robin similar to the one in
	// workload/driver.go.
	if len(urls) != 1 {
		return workload.QueryLoad{}, errors.Errorf(
			"Exactly one connection string is supported at the moment.")
	}
	connCfg, err := pgx.ParseConnectionString(urls[0])
	if err != nil {
		return workload.QueryLoad{}, err
	}
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	if w.querybenchPath != `` {
		conn, err := pgx.Connect(connCfg)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		worker := newQuerybenchWorker(w, reg, conn, 0 /* id */)
		ql.WorkerFns = append(ql.WorkerFns, worker.querybenchRun)
		return ql, nil
	}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		conn, err := pgx.Connect(connCfg)
		if err != nil {
			return workload.QueryLoad{}, err
		}
		worker := newWorker(w, reg, conn, i)
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	return ql, nil
}

type worker struct {
	config *querylog
	hists  *histogram.Histograms
	// We're using pgx.Conn to make sure that statement_timeout variable (when
	// applicable) is properly set on every connection.
	conn *pgx.Conn
	id   int
	rng  *rand.Rand

	reWriteQuery *regexp.Regexp

	querybenchPath string
}

func newWorker(q *querylog, reg *histogram.Registry, conn *pgx.Conn, id int) *worker {
	return &worker{
		config:       q,
		hists:        reg.GetHandle(),
		conn:         conn,
		id:           id,
		rng:          rand.New(rand.NewSource(q.seed + int64(id))),
		reWriteQuery: regexp.MustCompile(regexWriteQueryPattern),
	}
}

func newQuerybenchWorker(q *querylog, reg *histogram.Registry, conn *pgx.Conn, id int) *worker {
	w := newWorker(q, reg, conn, id)
	w.querybenchPath = q.querybenchPath
	return w
}

// run is the main function of the worker in which it chooses a query, attempts
// to generate values for the placeholders, and executes the query. Most errors
// (if any occur) are "swallowed" and do not stop the worker. The worker will
// only stop if the workload will tell it so (upon reaching the desired
// duration).
func (w *worker) run(ctx context.Context) error {
	if w.config.stmtTimeoutSeconds != 0 {
		if _, err := w.conn.Exec(fmt.Sprintf("SET statement_timeout='%ds'", w.config.stmtTimeoutSeconds)); err != nil {
			return err
		}
	}

	var start time.Time
	for {
		chosenQuery, tableName := w.chooseQuery()
		pholdersColumnNames, numRepeats, err := w.deduceColumnNamesForPlaceholders(ctx, chosenQuery)
		if err != nil {
			if w.config.verbose {
				log.Infof(ctx, "Encountered an error %s while deducing column names corresponding to the placeholders", err.Error())
				printQueryShortened(ctx, chosenQuery)
			}
			continue
		}

		placeholders, err := w.generatePlaceholders(ctx, chosenQuery, pholdersColumnNames, numRepeats, tableName)
		if err != nil {
			if w.config.verbose {
				log.Infof(ctx, "Encountered an error %s while generating values for the placeholders", err.Error())
				printQueryShortened(ctx, chosenQuery)
			}
			continue
		}

		start = timeutil.Now()
		rows, err := w.conn.Query(chosenQuery, placeholders...)
		if err != nil {
			if w.config.verbose {
				log.Infof(ctx, "Encountered an error %s while executing the query", err.Error())
				printQueryShortened(ctx, chosenQuery)
			}
			continue
		}
		// TODO(yuzefovich): do we care about the returned rows?
		// Iterate over all rows to simulate regular behavior.
		for rows.Next() {
		}
		rows.Close()
		elapsed := timeutil.Since(start)
		// TODO(yuzefovich): is there a better way to display the results?
		w.hists.Get("").Record(elapsed)
	}
}

// chooseQuery chooses a random query found in the query log. The queries are
// chosen proportionally likely to the frequency of their occurrence in the
// query log.
func (w *worker) chooseQuery() (chosenQuery string, tableName string) {
	prob := w.rng.Float64()
	count := 0
	for tableIdx, tableName := range w.config.state.tableNames {
		if probInInterval(count, w.config.state.queryCountPerTable[tableIdx], w.config.state.totalQueryCount, prob) {
			countForThisTable := 0
			totalForThisTable := w.config.state.queryCountPerTable[tableIdx]
			for query, frequency := range w.config.state.seenQueriesByTableName[tableIdx] {
				if probInInterval(countForThisTable, frequency, totalForThisTable, prob) {
					return query, tableName
				}
				countForThisTable += frequency
			}
		}
		count += w.config.state.queryCountPerTable[tableIdx]
	}
	// We should've chosen a query in the loop. Just in case we encountered some
	// very unlikely rounding errors, let's return some query here.
	for tableIdx, tableName := range w.config.state.tableNames {
		if w.config.state.queryCountPerTable[tableIdx] > 0 {
			for query := range w.config.state.seenQueriesByTableName[tableIdx] {
				return query, tableName
			}
		}
	}
	panic("no queries were accumulated from the log")
}

// deduceColumnNamesForPlaceholders attempts to deduce the names of
// corresponding to placeholders columns. numRepeats indicates how many times
// that the same "schema" of columns should be repeated when generating the
// values for placeholders - it is 1 for read queries and can be more than 1
// for write queries.
func (w *worker) deduceColumnNamesForPlaceholders(
	ctx context.Context, query string,
) (pholdersColumnNames []string, numRepeats int, err error) {
	if isInsertOrUpsert(query) {
		// Write query is chosen. We assume that the format is:
		// INSERT/UPSERT INTO table(col1, col2, ..., coln) VALUES ($1, $2, ..., $n), ($n+1, $n+2, ..., $n+n), ... ($kn+1, $kn+2, ..., $kn+n)
		if !w.reWriteQuery.Match([]byte(query)) {
			return nil, 0, errors.Errorf("Chosen write query didn't match the pattern.")
		}
		submatch := w.reWriteQuery.FindSubmatch([]byte(query))
		columnsIdx := 3
		columnsNames := string(submatch[columnsIdx])
		pholdersColumnNames = strings.FieldsFunc(columnsNames, func(r rune) bool {
			return r == ' ' || r == ','
		})
		lastPholderIdx := strings.LastIndex(query, "$")
		if lastPholderIdx == -1 {
			return nil, 0, errors.Errorf("Unexpected: no placeholders in the write query.")
		}
		multipleValuesGroupIdx := 4
		// numRepeats should equal to the number of value tuples in the query, so
		// we calculate it as the number of '(' seen in the multiple values group
		// in the regular expression (which can be any non-negative integer) plus 1
		// (for the last tuple that must be present).
		numRepeats = 1 + strings.Count(string(submatch[multipleValuesGroupIdx]), "(")
		return pholdersColumnNames, numRepeats, nil
	}

	// Read query is chosen. We're making best effort to deduce the column name,
	// namely we assume that all placeholders are used in an expression as follows
	// `col <> $1`, i.e. a column name followed by a sign followed by the
	// placeholder.
	pholdersColumnNames = make([]string, 0)
	for i := 1; ; i++ {
		pholder := fmt.Sprintf("$%d", i)
		pholderIdx := strings.Index(query, pholder)
		if pholderIdx == -1 {
			// We have gone over all the placeholders present in the query.
			break
		}
		tokens := strings.Fields(query[:pholderIdx])
		if len(tokens) < 2 {
			return nil, 0, errors.Errorf("assumption that there are at least two tokens before placeholder is wrong")
		}
		column := tokens[len(tokens)-2]
		for column[0] == '(' {
			column = column[1:]
		}
		pholdersColumnNames = append(pholdersColumnNames, column)
	}
	return pholdersColumnNames, 1 /* numRepeats */, nil
}

// generatePlaceholders populates the values for the placeholders. It utilizes
// two strategies:
// 1. select random samples based on the samples stored in columnInfos.
// 2. generate random values based on the types of the corresponding columns.
// Note: if a deduced column name corresponding to a placeholder is not among
// the columns of the table, we assume that it is of INT type and generate an
// int in [1, 10] range.
func (w *worker) generatePlaceholders(
	ctx context.Context, query string, pholdersColumnNames []string, numRepeats int, tableName string,
) (placeholders []interface{}, err error) {
	isWriteQuery := isInsertOrUpsert(query)
	placeholders = make([]interface{}, 0, len(pholdersColumnNames)*numRepeats)
	for j := 0; j < numRepeats; j++ {
		for i, column := range pholdersColumnNames {
			columnMatched := false
			actualTableName := true
			if strings.Contains(column, ".") {
				actualTableName = false
				column = strings.Split(column, ".")[1]
			}
			possibleTableNames := make([]string, 0, 1)
			possibleTableNames = append(possibleTableNames, tableName)
			if !actualTableName {
				// column comes from a table that was aliased. In order to not parse
				// the query to figure out actual table name, we simply compare to
				// columns of all used tables giving priority to the table that the
				// query is assigned to.
				for _, n := range w.config.state.tableNames {
					if n == tableName || !w.config.state.tableUsed[n] {
						continue
					}
					possibleTableNames = append(possibleTableNames, n)
				}
			}
			for _, tableName := range possibleTableNames {
				if columnMatched {
					break
				}
				for _, c := range w.config.state.columnsByTableName[tableName] {
					if c.name == column {
						if w.rng.Float64() < w.config.probOfUsingSamples && c.samples != nil && !isWriteQuery {
							// For non-write queries when samples are present, we're using
							// the samples with w.config.probOfUsingSamples probability.
							sampleIdx := w.rng.Intn(len(c.samples))
							placeholders = append(placeholders, c.samples[sampleIdx])
						} else {
							// In all other cases, we generate random values for the
							// placeholders.
							nullPct := 0
							if c.isNullable && w.config.nullPct > 0 {
								nullPct = 100 / w.config.nullPct
							}
							d := randgen.RandDatumWithNullChance(w.rng, c.dataType, nullPct)
							if i, ok := d.(*tree.DInt); ok && c.intRange > 0 {
								j := int64(*i) % int64(c.intRange/2)
								d = tree.NewDInt(tree.DInt(j))
							}
							p, err := workloadrand.DatumToGoSQL(d)
							if err != nil {
								return nil, err
							}
							placeholders = append(placeholders, p)
						}
						columnMatched = true
						break
					}
				}
			}
			if !columnMatched {
				d := w.rng.Int31n(10) + 1
				if w.config.verbose {
					log.Infof(ctx, "Couldn't deduce the corresponding to $%d, so generated %d (a small int)", i+1, d)
					printQueryShortened(ctx, query)
				}
				p, err := workloadrand.DatumToGoSQL(tree.NewDInt(tree.DInt(d)))
				if err != nil {
					return nil, err
				}
				placeholders = append(placeholders, p)
			}
		}
	}
	return placeholders, nil
}

// getTableNames fetches the names of all the tables in db and stores them in
// w.state.
func (w *querylog) getTableNames(db *gosql.DB) error {
	rows, err := db.Query(`SELECT table_name FROM [SHOW TABLES] ORDER BY table_name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	w.state.tableNames = make([]string, 0)
	for rows.Next() {
		var tableName string
		if err = rows.Scan(&tableName); err != nil {
			return err
		}
		w.state.tableNames = append(w.state.tableNames, tableName)
	}
	return nil
}

// unzip unzips the zip file at src into dest. It was copied (with slight
// modifications) from
// https://stackoverflow.com/questions/20357223/easy-way-to-unzip-file-with-golang.
func unzip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	if err = os.MkdirAll(dest, 0755); err != nil {
		return err
	}

	// Closure to address file descriptors issue with all the deferred .Close()
	// methods.
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)
		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(path, filepath.Clean(dest)+string(os.PathSeparator)) {
			return errors.Errorf("%s: illegal file path while extracting the zip. "+
				"Such a file path can be dangerous because of ZipSlip vulnerability. "+
				"Please reconsider whether the zip file is trustworthy.", path)
		}

		if f.FileInfo().IsDir() {
			if err = os.MkdirAll(path, f.Mode()); err != nil {
				return err
			}
		} else {
			if err = os.MkdirAll(filepath.Dir(path), f.Mode()); err != nil {
				return err
			}
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}

const (
	// omitted prefix + "(query)" + \s + (placeholders) + omitted suffix
	regexQueryLogFormat = `[^\s]+\s[^\s]+\s[^\s]+\s[^\s]+\s{2}[^\s]+\s[^\s]+\s[^\s]+\s".*"\s\{.*\}\s` +
		`"(.*)"` + `\s` + `(\{.*\})` + `\s[^\s]+\s[\d]+\s[^\s]+`

	regexQueryGroupIdx = 1
	//regexPlaceholdersGroupIdx = 2
	queryLogHeaderLines = 5

	regexWriteQueryPattern = `(UPSERT|INSERT)\sINTO\s([^\s]+)\((.*)\)\sVALUES\s(\(.*\)\,\s)*(\(.*\))`
)

// parseFile parses the file one line at a time. First queryLogHeaderLines are
// skipped, and all other lines are expected to match the pattern of re.
func (w *querylog) parseFile(ctx context.Context, fileInfo os.FileInfo, re *regexp.Regexp) error {
	start := timeutil.Now()
	file, err := os.Open(w.dirPath + "/" + fileInfo.Name())
	if err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	buffer := make([]byte, 0)
	lineCount := 0
	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if err != io.EOF {
				return err
			}
			// We reached EOF, so we're done with this file.
			end := timeutil.Now()
			elapsed := end.Sub(start)
			log.Infof(ctx, "Processing of %s is done in %fs", fileInfo.Name(), elapsed.Seconds())
			break
		}

		buffer = append(buffer, line...)
		if !isPrefix {
			// We read the full line, so let's process it.
			if lineCount >= queryLogHeaderLines {
				// First queryLogHeaderLines lines of the describe the format of the log
				// file, so we skip them.
				if !re.Match(buffer) {
					return errors.Errorf("Line %d doesn't match the pattern", lineCount)
				}
				// regexQueryLogFormat contains two regexp groups (patterns that are
				// in grouping parenthesis) - the query (without quotes) and actual
				// placeholder values. At the moment, we're only interested in the
				// former.
				groups := re.FindSubmatch(buffer)
				query := string(groups[regexQueryGroupIdx])
				isWriteQuery := isInsertOrUpsert(query)
				skipQuery := strings.HasPrefix(query, "ALTER") ||
					(w.omitWriteQueries && isWriteQuery) ||
					(w.omitDeleteQueries && strings.HasPrefix(query, "DELETE"))
				if !skipQuery {
					tableAssigned := false
					for i, tableName := range w.state.tableNames {
						// TODO(yuzefovich): this simplistic matching doesn't work in all
						// cases.
						if strings.Contains(query, tableName) {
							if !tableAssigned {
								// The query is assigned to one table, namely the one that
								// showed up earlier in `SHOW TABLES`.
								w.state.seenQueriesByTableName[i][query]++
								w.state.queryCountPerTable[i]++
								w.state.totalQueryCount++
								tableAssigned = true
							}
							w.state.tableUsed[tableName] = true
						}
					}
					if !tableAssigned {
						return errors.Errorf("No table matched query %s while processing %s", query, fileInfo.Name())
					}
				}
			}
			buffer = buffer[:0]
			lineCount++
		}
	}
	return file.Close()
}

// processQueryLog parses the query log and populates most of w.state.
func (w *querylog) processQueryLog(ctx context.Context) error {
	if w.zipPath != "" {
		log.Infof(ctx, "About to start unzipping %s", w.zipPath)
		w.dirPath = w.zipPath[:len(w.zipPath)-4]
		if err := unzip(w.zipPath, w.dirPath); err != nil {
			return err
		}
		log.Infof(ctx, "Unzipping to %s is complete", w.dirPath)
	}

	files, err := ioutil.ReadDir(w.dirPath)
	if err != nil {
		return err
	}

	w.state.queryCountPerTable = make([]int, len(w.state.tableNames))
	w.state.seenQueriesByTableName = make([]map[string]int, len(w.state.tableNames))
	for i := range w.state.tableNames {
		w.state.seenQueriesByTableName[i] = make(map[string]int)
	}
	w.state.tableUsed = make(map[string]bool)

	re := regexp.MustCompile(regexQueryLogFormat)
	log.Infof(ctx, "Starting to parse the query log")
	numFiles := w.filesToParse
	if numFiles > len(files) {
		numFiles = len(files)
	}
	for fileNum, fileInfo := range files {
		if fileNum == w.filesToParse {
			// We have reached the desired number of files to parse, so we don't
			// parse the remaining files.
			break
		}
		if fileInfo.IsDir() {
			if w.verbose {
				log.Infof(ctx, "Unexpected: a directory %s is encountered with the query log, skipping it.", fileInfo.Name())
			}
			continue
		}
		log.Infof(ctx, "Processing %d out of %d", fileNum, numFiles)
		if err = w.parseFile(ctx, fileInfo, re); err != nil {
			return err
		}
	}
	log.Infof(ctx, "Query log processed")
	if w.zipPath != "" {
		log.Infof(ctx, "Unzipped files are about to be removed")
		return os.RemoveAll(w.dirPath)
	}
	return nil
}

// getColumnsInfo populates the information about the columns of the tables
// that at least one query was issued against.
func (w *querylog) getColumnsInfo(db *gosql.DB) error {
	w.state.columnsByTableName = make(map[string][]columnInfo)
	for _, tableName := range w.state.tableNames {
		if !w.state.tableUsed[tableName] {
			// There were no queries operating on this table, so no query will be
			// generated against this table as well, and we don't need the
			// information about the columns.
			continue
		}

		// columnTypeByColumnName is used only to distinguish between
		// INT2/INT4/INT8 because otherwise they are mapped to the same INT type.
		columnTypeByColumnName := make(map[string]string)
		rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type FROM [SHOW COLUMNS FROM %s]", tableName))
		if err != nil {
			return err
		}
		for rows.Next() {
			var columnName, dataType string
			if err = rows.Scan(&columnName, &dataType); err != nil {
				return err
			}
			columnTypeByColumnName[columnName] = dataType
		}
		if err = rows.Close(); err != nil {
			return err
		}

		// This schema introspection was copied from workload/rand.go (with slight
		// modifications).
		// TODO(yuzefovich): probably we need to extract it.
		var relid int
		if err := db.QueryRow(fmt.Sprintf("SELECT '%s'::REGCLASS::OID", tableName)).Scan(&relid); err != nil {
			return err
		}
		rows, err = db.Query(
			`
SELECT attname, atttypid, adsrc, NOT attnotnull
FROM pg_catalog.pg_attribute
LEFT JOIN pg_catalog.pg_attrdef
ON attrelid=adrelid AND attnum=adnum
WHERE attrelid=$1`, relid)
		if err != nil {
			return err
		}

		var cols []columnInfo
		var numCols = 0

		defer rows.Close()
		for rows.Next() {
			var c columnInfo
			c.dataPrecision = 0
			c.dataScale = 0

			var typOid int
			if err := rows.Scan(&c.name, &typOid, &c.cdefault, &c.isNullable); err != nil {
				return err
			}
			c.dataType = types.OidToType[oid.Oid(typOid)]
			if c.dataType.Family() == types.IntFamily {
				actualType := columnTypeByColumnName[c.name]
				if actualType == `INT2` {
					c.intRange = 1 << 16
				} else if actualType == `INT4` {
					c.intRange = 1 << 32
				}
			}
			cols = append(cols, c)
			numCols++
		}

		if numCols == 0 {
			return errors.Errorf("no columns detected")
		}
		w.state.columnsByTableName[tableName] = cols
	}
	return nil
}

// populateSamples selects at most w.numSamples of samples from each table that
// at least one query was issued against the query log. The samples are stored
// inside corresponding to the table columnInfo.
func (w *querylog) populateSamples(ctx context.Context, db *gosql.DB) error {
	log.Infof(ctx, "Populating samples started")
	for _, tableName := range w.state.tableNames {
		cols := w.state.columnsByTableName[tableName]
		if cols == nil {
			// There were no queries touching this table, so we skip it.
			continue
		}
		log.Infof(ctx, "Sampling %s", tableName)
		count, err := db.Query(fmt.Sprintf(`SELECT count(*) FROM %s`, tableName))
		if err != nil {
			return err
		}
		count.Next()
		var numRows int
		if err = count.Scan(&numRows); err != nil {
			return err
		}
		if err = count.Close(); err != nil {
			return err
		}

		// To ensure that samples correspond to the appropriate columns, we fix
		// the order of columns (same to the order in `SHOW COLUMNS` query in
		// getColumnInfo).
		columnNames := make([]string, len(cols))
		for i := range cols {
			columnNames[i] = cols[i].name
		}
		columnsOrdered := strings.Join(columnNames, ", ")

		var samples *gosql.Rows
		if w.numSamples > numRows {
			samples, err = db.Query(
				fmt.Sprintf(`SELECT %s FROM %s`, columnsOrdered, tableName))
		} else {
			samplingProb := float64(w.numSamples) / float64(numRows)
			if samplingProb < w.minSamplingProb {
				// To speed up the query.
				samplingProb = w.minSamplingProb
			}
			samples, err = db.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE random() < %f LIMIT %d`,
				columnsOrdered, tableName, samplingProb, w.numSamples))
		}

		if err != nil {
			return err
		}
		for samples.Next() {
			rowOfSamples := make([]interface{}, len(cols))
			for i := range rowOfSamples {
				rowOfSamples[i] = new(interface{})
			}
			if err := samples.Scan(rowOfSamples...); err != nil {
				return err
			}
			for i, sample := range rowOfSamples {
				cols[i].samples = append(cols[i].samples, sample)
			}
		}
		if err = samples.Close(); err != nil {
			return err
		}
	}
	log.Infof(ctx, "Populating samples is complete")
	return nil
}

// querybenchRun is the main function of a querybench worker. It is run only
// when querylog is used in querybench mode (i.e. querybench-path argument was
// specified). It chooses a query, attempts to generate values for the
// placeholders, and - instead of executing the query - writes it into the
// requested file in format that querybench understands.
func (w *worker) querybenchRun(ctx context.Context) error {
	file, err := os.Create(w.querybenchPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// We will skip all queries for which the placeholder values contain `$1`
	// and alike to make it easier to replace actual placeholders.
	reToAvoid := regexp.MustCompile(`\$[0-9]+`)
	writer := bufio.NewWriter(file)
	queryCount := uint(0)
	for {
		chosenQuery, tableName := w.chooseQuery()
		pholdersColumnNames, numRepeats, err := w.deduceColumnNamesForPlaceholders(ctx, chosenQuery)
		if err != nil {
			if w.config.verbose {
				log.Infof(ctx, "Encountered an error %s while deducing column names corresponding to the placeholders", err.Error())
				printQueryShortened(ctx, chosenQuery)
			}
			continue
		}

		placeholders, err := w.generatePlaceholders(ctx, chosenQuery, pholdersColumnNames, numRepeats, tableName)
		if err != nil {
			if w.config.verbose {
				log.Infof(ctx, "Encountered an error %s while generating values for the placeholders", err.Error())
				printQueryShortened(ctx, chosenQuery)
			}
			continue
		}

		query := chosenQuery
		skipQuery := false
		// We're iterating over placeholders in reverse order so that `$1` does not
		// replace the first two characters of `$10`.
		for i := len(placeholders) - 1; i >= 0; i-- {
			pholderString := fmt.Sprintf("$%d", i+1)
			if !strings.Contains(chosenQuery, pholderString) {
				skipQuery = true
				break
			}
			pholderValue := printPlaceholder(placeholders[i])
			if reToAvoid.MatchString(pholderValue) {
				skipQuery = true
				break
			}
			query = strings.Replace(query, pholderString, pholderValue, -1)
		}
		if skipQuery {
			if w.config.verbose {
				log.Infof(ctx, "Could not replace placeholders with values on query")
				printQueryShortened(ctx, chosenQuery)
			}
			continue
		}
		if _, err = writer.WriteString(query + "\n\n"); err != nil {
			return err
		}

		queryCount++
		if queryCount%250 == 0 {
			log.Infof(ctx, "%d queries have been written", queryCount)
		}
		if queryCount == w.config.count {
			writer.Flush()
			return nil
		}
	}
}

func printPlaceholder(i interface{}) string {
	if ptr, ok := i.(*interface{}); ok {
		return printPlaceholder(*ptr)
	}
	switch p := i.(type) {
	case bool:
		return fmt.Sprintf("%v", p)
	case int64:
		return fmt.Sprintf("%d", p)
	case float64:
		return fmt.Sprintf("%f", p)
	case []uint8:
		u, err := uuid.FromString(string(p))
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("'%s'", u.String())
	case uuid.UUID:
		return fmt.Sprintf("'%s'", p.String())
	case string:
		s := strings.Replace(p, "'", "''", -1)
		// querybench assumes that each query is on a single line, so we remove
		// line breaks.
		s = strings.Replace(s, "\n", "", -1)
		s = strings.Replace(s, "\r", "", -1)
		return fmt.Sprintf("'%s'", s)
	case time.Time:
		timestamp := p.String()
		// timestamp can be of the format '1970-01-09 00:14:01.000812453 +0000 UTC'
		// or '2019-02-26 20:52:01.65434 +0000 +0000', and the parser complains
		// that it could not parse it, so we remove all stuff that comes after
		// first +0000 to make the parser happy.
		idx := strings.Index(timestamp, `+0000`)
		timestamp = timestamp[:idx+5]
		return fmt.Sprintf("'%s':::TIMESTAMP", timestamp)
	case nil:
		return "NULL"
	default:
		panic(errors.AssertionFailedf("unsupported type: %T", i))
	}
}

// TODO(yuzefovich): columnInfo is copied from workload/rand package and
// extended. Should we export workload/rand.col?
type columnInfo struct {
	name          string
	dataType      *types.T
	dataPrecision int
	dataScale     int
	cdefault      gosql.NullString
	isNullable    bool

	intRange uint64 // To distinguish between INT2, INT4, and INT8.
	samples  []interface{}
}

func printQueryShortened(ctx context.Context, query string) {
	if len(query) > 1000 {
		log.Infof(ctx, "%s...%s", query[:500], query[len(query)-500:])
	} else {
		log.Infof(ctx, "%s", query)
	}
}

func isInsertOrUpsert(query string) bool {
	return strings.HasPrefix(query, "INSERT") || strings.HasPrefix(query, "UPSERT")
}

func probInInterval(start, inc, total int, p float64) bool {
	return float64(start)/float64(total) <= p &&
		float64(start+inc)/float64(total) > p
}
