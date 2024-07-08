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
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/google"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/slack-go/slack"
	"golang.org/x/exp/maps"
	"golang.org/x/perf/benchfmt"
)

type compareConfig struct {
	newDir       string
	oldDir       string
	sheetDesc    string
	slackUser    string
	slackChannel string
	slackToken   string
	threshold    float64
}

type compare struct {
	compareConfig
	service  *google.Service
	packages []string
	ctx      context.Context
}

const (
	packageSeparator         = "→"
	slackPercentageThreshold = 20.0
	slackReportMax           = 3
	skipComparison           = math.MaxFloat64
)

type PackageComparison struct {
	PackageName string
	Metrics     []MetricComparison
}

type MetricComparison struct {
	MetricKey string
	Result    string
}

type ComparisonReport struct {
	PackageComparisons []PackageComparison
}

const slackCompareTemplateScript = `
{{ range .Metrics }}*Top {{ len .Changes }} significant change(s) for metric: {{ .MetricName }}*
{{ range .Changes }}• {{ .BenchmarkName }} {{.ChangeSymbol}}{{ .PercentChange }}
{{ end }}
{{ end }}
`

func newCompare(config compareConfig) (*compare, error) {
	// Use the old directory to infer package info.
	packages, err := getPackagesFromLogs(config.oldDir)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	var service *google.Service
	if config.threshold == skipComparison {
		service, err = google.New(ctx)
		if err != nil {
			return nil, err
		}
	}
	return &compare{compareConfig: config, service: service, packages: packages, ctx: ctx}, nil
}

func defaultCompareConfig() compareConfig {
	return compareConfig{
		threshold:    skipComparison, // Skip comparison by default
		slackUser:    "microbench",
		slackChannel: "perf-ops",
	}
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

func (c *compare) createComparisons(
	metricMap model.MetricMap, oldID string, newID string,
) map[string]map[string]*model.Comparison {

	metricKeys := make([]string, 0, len(metricMap))
	for sheetName := range metricMap {
		metricKeys = append(metricKeys, sheetName)
	}

	comparisonMap := make(map[string]map[string]*model.Comparison, len(metricKeys))
	for metricKey, metric := range metricMap {

		// Compute comparisons for each benchmark present in both runs.
		comparisons := make(map[string]*model.Comparison)
		for name := range metric.BenchmarkEntries {
			comparison := metric.ComputeComparison(name, oldID, newID)
			if comparison != nil {
				comparisons[name] = comparison
			}
		}

		if len(comparisons) != 0 {
			comparisonMap[metricKey] = comparisons
		}
	}

	return comparisonMap
}

func (c *compare) publishToGoogleSheets(
	metricMaps map[string]*model.MetricMap,
) (map[string]string, error) {
	sheets := make(map[string]string)
	for pkgGroup, metricMap := range metricMaps {
		sheetName := pkgGroup + "/..."
		if c.sheetDesc != "" {
			sheetName = fmt.Sprintf("%s (%s)", sheetName, c.sheetDesc)
		}

		comparisonMap := c.createComparisons(*metricMap, "old", "new")
		url, err := c.service.CreateSheet(c.ctx, sheetName, *metricMap, comparisonMap, "old", "new")
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create sheet for %s", pkgGroup)
		}
		log.Printf("Generated sheet for %s: %s\n", sheetName, url)
		sheets[pkgGroup] = url
	}
	return sheets, nil
}

func (c *compare) postToSlack(
	links map[string]string, metricMaps map[string]*model.MetricMap,
) error {
	// Template structures used to generate the Slack message.
	type changeInfo struct {
		BenchmarkName string
		PercentChange string
		ChangeSymbol  string
	}
	type metricInfo struct {
		MetricName string
		Changes    []changeInfo
	}

	pkgGroups := maps.Keys(metricMaps)
	sort.Strings(pkgGroups)
	var attachments []slack.Attachment
	for _, pkgGroup := range pkgGroups {
		metricMap := metricMaps[pkgGroup]
		metricKeys := maps.Keys(*metricMap)
		sort.Sort(sort.Reverse(sort.StringSlice(metricKeys)))
		metrics := make([]metricInfo, 0)
		var highestPercentChange = 0.0
		for _, metricKey := range metricKeys {
			metric := (*metricMap)[metricKey]
			mi := metricInfo{MetricName: metric.Name}

			// Compute comparisons for each benchmark present in both runs.
			comparisons := make(map[string]*model.Comparison)
			for name := range metric.BenchmarkEntries {
				comparison := metric.ComputeComparison(name, "old", "new")
				if comparison != nil {
					comparisons[name] = comparison
				}
			}

			// Sort comparisons by delta, or the benchmark name if no delta is available.
			keys := maps.Keys(comparisons)
			sort.Slice(keys, func(i, j int) bool {
				d1 := comparisons[keys[i]].Delta * float64(metric.Better)
				d2 := comparisons[keys[j]].Delta * float64(metric.Better)
				if d1 == d2 {
					return keys[i] < keys[j]
				}
				return d1 < d2
			})

			for _, name := range keys {
				if len(mi.Changes) >= slackReportMax {
					break
				}
				if (comparisons[name].Delta < 0 && metric.Better < 0) ||
					(comparisons[name].Delta > 0 && metric.Better > 0) ||
					comparisons[name].Delta == 0 {
					continue
				}
				nameSplit := strings.Split(name, packageSeparator)
				ci := changeInfo{
					BenchmarkName: nameSplit[0] + packageSeparator + truncateBenchmarkName(nameSplit[1], 32),
					PercentChange: fmt.Sprintf("%.2f%%", comparisons[name].Delta),
				}
				if math.Abs(comparisons[name].Delta) > highestPercentChange {
					highestPercentChange = math.Abs(comparisons[name].Delta)
				}
				ci.ChangeSymbol = ":small_orange_diamond:"
				if math.Abs(comparisons[name].Delta) > slackPercentageThreshold {
					ci.ChangeSymbol = ":small_red_triangle:"
				}
				mi.Changes = append(mi.Changes, ci)
			}
			if len(mi.Changes) > 0 {
				metrics = append(metrics, mi)
			}
		}
		status := "good"
		output := "No significant changes."
		if len(metrics) > 0 {
			var sb strings.Builder
			t, err := template.New("summary").Parse(slackCompareTemplateScript)
			if err != nil {
				return err
			}
			err = t.Execute(&sb, struct{ Metrics []metricInfo }{metrics})
			if err != nil {
				return err
			}
			status = "warning"
			if highestPercentChange > slackPercentageThreshold {
				status = "danger"
			}
			output = sb.String()
		}
		link := links[pkgGroup]
		attachments = append(attachments,
			slack.Attachment{
				Color:   status,
				Pretext: fmt.Sprintf("<%s|Google Sheet> for *%s/...*", link, pkgGroup),
				Text:    output,
			})

	}

	s := newSlackClient(c.slackUser, c.slackChannel, c.slackToken)
	return s.Post(
		slack.MsgOptionText(fmt.Sprintf("Microbenchmark comparison summary: %s", c.sheetDesc), false),
		slack.MsgOptionAttachments(attachments...),
	)
}

func (r *ComparisonReport) getReportString() string {
	var builder strings.Builder
	for _, pkgError := range r.PackageComparisons {
		builder.WriteString(fmt.Sprintf("Package: %s\n", pkgError.PackageName))
		for _, metricError := range pkgError.Metrics {
			builder.WriteString(fmt.Sprintf("\tMetric: %s, Result: %s\n", metricError.MetricKey, metricError.Result))
		}
	}
	return builder.String()
}

func (c *compare) compareUsingThreshold(metricMap map[string]*model.MetricMap) error {

	report := ComparisonReport{}

	for pkgName, metricMap := range metricMap {
		var metrics []MetricComparison
		comparisonMap := c.createComparisons(*metricMap, "old", "new")

		for metricKey, comparisons := range comparisonMap {
			for benchmarkName, comparison := range comparisons {
				// If Delta is more negative than the threshold, then there's a concerning perf regression
				if comparison.Delta+(c.threshold*100) < 0 {
					metrics = append(metrics, MetricComparison{
						MetricKey: metricKey,
						Result:    fmt.Sprintf("%s : %s", benchmarkName, comparison.FormattedDelta),
					})
				}
			}
		}

		if len(metrics) > 0 {
			report.PackageComparisons = append(report.PackageComparisons, PackageComparison{
				PackageName: pkgName,
				Metrics:     metrics,
			})
		}
	}

	if len(report.PackageComparisons) > 0 {
		reportString := report.getReportString()
		return errors.Newf("There are benchmark regressions of > %.2f%% in the following packages \n %s\n",
			c.threshold*100, reportString)
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
	return builder.AddMetrics(id, pkg+packageSeparator, reader)
}

func truncateBenchmarkName(text string, maxLen int) string {
	lastSlash := maxLen
	curLen := 0
	for i, r := range text {
		if r == '/' {
			lastSlash = i
		}
		curLen++
		if curLen > maxLen {
			return text[:lastSlash] + "..."
		}
	}
	return text
}
