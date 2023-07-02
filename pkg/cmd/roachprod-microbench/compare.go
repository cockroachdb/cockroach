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
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/google"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"golang.org/x/perf/benchfmt"
)

type compareConfig struct {
	newDir    string
	oldDir    string
	sheetDesc string
}

type compare struct {
	compareConfig
	service  *google.Service
	packages []string
	ctx      context.Context
}

func newCompare(config compareConfig) (*compare, error) {
	// Use the old directory to infer package info.
	packages, err := getPackagesFromLogs(config.oldDir)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	service, err := google.New(ctx)
	if err != nil {
		return nil, err
	}
	return &compare{compareConfig: config, service: service, packages: packages, ctx: ctx}, nil
}

func (c *compare) readMetrics() (map[string]*model.MetricMap, error) {
	builders := make(map[string]*model.Builder)
	for _, pkg := range c.packages {
		basePackage := pkg[:strings.Index(pkg[4:]+"/", "/")+4]
		results, ok := builders[basePackage]
		if !ok {
			results = model.NewBuilder()
			builders[basePackage] = results
		}

		// Read the previous and current results. If either is missing, we'll just
		// skip it.
		if err := processReportFile(results, "old", pkg,
			filepath.Join(c.oldDir, getReportLogName(reportLogName, pkg))); err != nil {
			return nil, err

		}
		if err := processReportFile(results, "new", pkg,
			filepath.Join(c.newDir, getReportLogName(reportLogName, pkg))); err != nil {
			log.Printf("failed to add report for %s: %s", pkg, err)
			return nil, err
		}
	}

	// Compute the results.
	metricMaps := make(map[string]*model.MetricMap)
	for pkg, builder := range builders {
		metricMap := builder.ComputeMetricMap()
		metricMaps[pkg] = &metricMap
	}
	return metricMaps, nil
}

func (c *compare) publishToGoogleSheets(metricMaps map[string]*model.MetricMap) error {
	for pkgGroup, metricMap := range metricMaps {
		sheetName := pkgGroup + "/..."
		if c.sheetDesc != "" {
			sheetName += " " + c.sheetDesc
		}
		url, err := c.service.CreateSheet(c.ctx, sheetName, *metricMap, "old", "new")
		if err != nil {
			return err
		}
		log.Printf("Generated sheet for %s: %s\n", sheetName, url)
	}
	return nil
}

func processReportFile(builder *model.Builder, id, pkg, path string) error {
	file, err := os.Open(path)
	if err != nil {
		// A not found error is ignored since it can be expected that
		// some microbenchmarks have changed names or been removed.
		if oserror.IsNotExist(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to create reader for %s", path)
	}
	defer file.Close()
	reader := benchfmt.NewReader(file, path)
	return builder.AddMetrics(id, pkg+"/", reader)
}
