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
	"golang.org/x/perf/benchstat"
	"golang.org/x/perf/storage/benchfmt"
)

func compareBenchmarks(
	packages []string, currentDir, previousDir string,
) (map[string][]*benchstat.Table, error) {
	packageResults := make(map[string][][]*benchfmt.Result)
	var resultMutex syncutil.Mutex
	var wg sync.WaitGroup
	errorsFound := false
	wg.Add(len(packages))
	for _, pkg := range packages {
		go func(pkg string) {
			defer wg.Done()
			basePackage := pkg[:strings.Index(pkg[4:]+"/", "/")+4]
			resultMutex.Lock()
			results, ok := packageResults[basePackage]
			if !ok {
				results = [][]*benchfmt.Result{make([]*benchfmt.Result, 0), make([]*benchfmt.Result, 0)}
				packageResults[basePackage] = results
			}
			resultMutex.Unlock()
			if err := addReportFile(&results[0], joinPath(previousDir, getReportLogName(reportLogName, pkg))); err != nil &&
				!isNotFoundError(err) {
				l.Errorf("failed to add report for %s: %s", pkg, err)
				errorsFound = true
			}
			if err := addReportFile(&results[1], joinPath(currentDir, getReportLogName(reportLogName, pkg))); err != nil &&
				!isNotFoundError(err) {
				l.Errorf("failed to add report for %s: %s", pkg, err)
				errorsFound = true
			}
			if results[0] == nil || results[1] == nil {
				resultMutex.Lock()
				delete(packageResults, basePackage)
				resultMutex.Unlock()
			}
		}(pkg)
	}
	wg.Wait()
	if errorsFound {
		return nil, errors.New("failed to process reports")
	}

	tableResults := make(map[string][]*benchstat.Table)
	for pkgGroup, results := range packageResults {
		var c benchstat.Collection
		c.Alpha = 0.05
		c.Order = benchstat.Reverse(benchstat.ByDelta)
		c.AddResults("old", results[0])
		c.AddResults("new", results[1])
		tables := c.Tables()
		tableResults[pkgGroup] = tables
	}
	return tableResults, nil
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

func addReportFile(results *[]*benchfmt.Result, path string) error {
	reader, err := createReader(path)
	if err != nil {
		return errors.Wrapf(err, "failed to create reader for %s", path)
	}
	br := benchfmt.NewReader(reader)
	for br.Next() {
		*results = append(*results, br.Result())
	}
	return br.Err()
}
