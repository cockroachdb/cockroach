// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/google"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	//lint:ignore SA1019 benchstat is deprecated
	"golang.org/x/perf/benchstat"
	//lint:ignore SA1019 storage/benchfmt is deprecated
	"golang.org/x/perf/storage/benchfmt"
)

func compareBenchmarks(
	packages []string, currentDir, previousDir string,
) (map[string][]*benchstat.Table, error) {
	type packageResults struct {
		old []*benchfmt.Result
		new []*benchfmt.Result
	}
	combinedResults := make(map[string]*packageResults)
	var resultMutex syncutil.Mutex
	var wg sync.WaitGroup
	errorsFound := false
	wg.Add(len(packages))
	for _, pkg := range packages {
		go func(pkg string) {
			defer wg.Done()
			basePackage := pkg[:strings.Index(pkg[4:]+"/", "/")+4]
			resultMutex.Lock()
			results, ok := combinedResults[basePackage]
			if !ok {
				results = &packageResults{}
				combinedResults[basePackage] = results
			}
			resultMutex.Unlock()

			// Read the previous and current results. If either is missing, we'll just
			// skip it. The not found error is ignored since it can be expected that
			// some benchmarks have changed names or been removed.
			if err := readReportFile(joinPath(previousDir, getReportLogName(reportLogName, pkg)),
				func(result *benchfmt.Result) {
					resultMutex.Lock()
					results.old = append(results.old, postfixResultWithPackage(pkg, result))
					resultMutex.Unlock()
				}); err != nil &&
				!isNotFoundError(err) {
				l.Errorf("failed to add report for %s: %s", pkg, err)
				errorsFound = true
			}
			if err := readReportFile(joinPath(currentDir, getReportLogName(reportLogName, pkg)),
				func(result *benchfmt.Result) {
					resultMutex.Lock()
					results.new = append(results.new, postfixResultWithPackage(pkg, result))
					resultMutex.Unlock()
				}); err != nil &&
				!isNotFoundError(err) {
				l.Errorf("failed to add report for %s: %s", pkg, err)
				errorsFound = true
			}
		}(pkg)
	}
	wg.Wait()
	if errorsFound {
		return nil, errors.New("failed to process reports")
	}

	tableResults := make(map[string][]*benchstat.Table)
	for pkgGroup, results := range combinedResults {
		var c benchstat.Collection
		c.Alpha = 0.05
		c.Order = benchstat.Reverse(benchstat.ByDelta)
		// Only add the results if both sets are present.
		if len(results.old) > 0 && len(results.new) > 0 {
			c.AddResults("old", results.old)
			c.AddResults("new", results.new)
			tables := prefixBenchmarkNamesWithPackage(c.Tables())
			tableResults[pkgGroup] = tables
		} else if len(results.old)+len(results.new) > 0 {
			l.Printf("Only one set of results present for %s", pkgGroup)
		}
	}
	return tableResults, nil
}

// postfixResultWithPackage appends the package name to the benchmark name
// following a special separator. This is done to avoid prefixing the benchmark
// name with the package name, as this would break the parsing of the benchmark
// name by benchstat further down the line.
func postfixResultWithPackage(pkg string, result *benchfmt.Result) *benchfmt.Result {
	fields := strings.Fields(result.Content)
	if !strings.HasPrefix(fields[0], "Benchmark") {
		return result
	}

	fields[0] = fields[0] + "*" + pkg
	return &benchfmt.Result{
		Labels:     result.Labels,
		NameLabels: result.NameLabels,
		LineNum:    result.LineNum,
		Content:    strings.Join(fields, " "),
	}
}

// prefixBenchmarkNamesWithPackage prefixes the benchmark name with the package
// name by using the post-fixing done in postfixResultWithPackage.
func prefixBenchmarkNamesWithPackage(tables []*benchstat.Table) []*benchstat.Table {
	for _, table := range tables {
		for _, row := range table.Rows {
			splitIndex := strings.LastIndex(row.Benchmark, "*")
			if splitIndex == -1 {
				continue
			}
			row.Benchmark = row.Benchmark[splitIndex+1:] + "/" + row.Benchmark[:splitIndex]
		}
	}
	return tables
}

func publishToGoogleSheets(
	ctx context.Context, srv *google.Service, sheetName string, tables []*benchstat.Table,
) error {
	url, err := srv.CreateSheet(ctx, sheetName, tables)
	if err != nil {
		return err
	}
	l.Printf("Generated sheet for %s: %s\n", sheetName, url)
	return nil
}

func readReportFile(path string, reportResults func(*benchfmt.Result)) error {
	reader, err := createReader(path)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader for %s", path)
	}
	br := benchfmt.NewReader(reader)
	for br.Next() {
		reportResults(br.Result())
	}
	return br.Err()
}
