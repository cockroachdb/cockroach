// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/google"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/model"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-microbench/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/slack-go/slack"
	"golang.org/x/exp/maps"
	"golang.org/x/perf/benchfmt"
	"golang.org/x/perf/benchseries"
)

type compareConfig struct {
	slackConfig   slackConfig
	influxConfig  influxConfig
	experimentDir string
	baselineDir   string
	sheetDesc     string
	threshold     float64
}

type slackConfig struct {
	user    string
	channel string
	token   string
}

type influxConfig struct {
	host     string
	token    string
	metadata map[string]string
}

type compare struct {
	compareConfig
	service  *google.Service
	packages []string
	ctx      context.Context
}

var defaultInfluxMetadata = map[string]string{
	"branch":      "master",
	"machine":     "n2-standard-32",
	"goarch":      "amd64",
	"goos":        "linux",
	"repository":  "cockroach",
	"run-time":    timeutil.Now().Format(time.RFC3339),
	"upload-time": timeutil.Now().Format(time.RFC3339),
}

const (
	slackPercentageThreshold = 20.0
	slackReportMax           = 3
	skipComparison           = math.MaxFloat64
)

const slackCompareTemplateScript = `
{{ range .Metrics }}*Top {{ len .Changes }} significant change(s) for metric: {{ .MetricName }}*
{{ range .Changes }}• {{ .BenchmarkName }} {{.ChangeSymbol}}{{ .PercentChange }}
{{ end }}
{{ end }}
`

func newCompare(config compareConfig) (*compare, error) {
	// Use the baseline directory to infer package info.
	packages, err := getPackagesFromLogs(config.baselineDir)
	if err != nil {
		return nil, err
	}
	// Add default metadata values to the influx config for any missing keys.
	for k, v := range defaultInfluxMetadata {
		if _, ok := config.influxConfig.metadata[k]; !ok {
			config.influxConfig.metadata[k] = v
		}
	}

	ctx := context.Background()
	var service *google.Service
	if config.sheetDesc != "" {
		service, err = google.New(ctx)
		if err != nil {
			return nil, err
		}
	}
	return &compare{compareConfig: config, service: service, packages: packages, ctx: ctx}, nil
}

func defaultCompareConfig() compareConfig {
	return compareConfig{
		threshold: skipComparison, // Skip comparison by default
		slackConfig: slackConfig{
			user:    "microbench",
			channel: "perf-ops",
		},
		influxConfig: influxConfig{
			host:     "http://localhost:8086",
			metadata: make(map[string]string),
		},
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
		if err := processReportFile(results, "baseline", pkg,
			filepath.Join(c.baselineDir, getReportLogName(reportLogName, pkg))); err != nil {
			return nil, err

		}
		if err := processReportFile(results, "experiment", pkg,
			filepath.Join(c.experimentDir, getReportLogName(reportLogName, pkg))); err != nil {
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
	metricMaps map[string]*model.MetricMap, oldID string, newID string,
) model.ComparisonResultsMap {

	comparisonResultsMap := make(model.ComparisonResultsMap)

	for pkgGroup, metricMap := range metricMaps {
		var comparisonResults []*model.ComparisonResult
		metricKeys := maps.Keys(*metricMap)
		sort.Sort(sort.Reverse(sort.StringSlice(metricKeys)))
		for _, metricKey := range metricKeys {
			metric := (*metricMap)[metricKey]
			// Compute comparisons for each benchmark present in both runs.
			comparisons := make(map[string]*model.Comparison)
			for name := range metric.BenchmarkEntries {
				comparison := metric.ComputeComparison(name, oldID, newID)
				if comparison != nil {
					comparisons[name] = comparison
				}
			}

			if len(comparisons) != 0 {
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

				var comparisonDetails []*model.ComparisonDetail
				for _, name := range keys {
					comparisonDetails = append(comparisonDetails, &model.ComparisonDetail{
						BenchmarkName: name,
						Comparison:    comparisons[name],
					})
				}

				comparisonResults = append(comparisonResults, &model.ComparisonResult{
					Metric:      metric,
					Comparisons: comparisonDetails,
				})
			}
		}

		comparisonResultsMap[pkgGroup] = comparisonResults
	}

	return comparisonResultsMap
}

func (c *compare) publishToGoogleSheets(
	comparisonResultsMap model.ComparisonResultsMap,
) (map[string]string, error) {
	sheets := make(map[string]string)
	for pkgGroup, comparisonResults := range comparisonResultsMap {
		sheetName := pkgGroup + "/..."
		if c.sheetDesc != "" {
			sheetName = fmt.Sprintf("%s (%s)", sheetName, c.sheetDesc)
		}

		url, err := c.service.CreateSheet(c.ctx, sheetName, comparisonResults, "baseline", "experiment")
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create sheet for %s", pkgGroup)
		}
		log.Printf("Generated sheet for %s: %s\n", sheetName, url)
		sheets[pkgGroup] = url
	}
	return sheets, nil
}

func (c *compare) postToSlack(
	links map[string]string, comparisonResultsMap model.ComparisonResultsMap,
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

	pkgGroups := maps.Keys(comparisonResultsMap)
	sort.Strings(pkgGroups)
	var attachments []slack.Attachment
	for _, pkgGroup := range pkgGroups {
		comparisonResults := comparisonResultsMap[pkgGroup]

		var metrics []metricInfo
		var highestPercentChange = 0.0
		for _, result := range comparisonResults {
			mi := metricInfo{MetricName: result.Metric.Name}

			for _, detail := range result.Comparisons {
				if len(mi.Changes) >= slackReportMax {
					break
				}
				comparison := detail.Comparison
				metric := result.Metric

				if (comparison.Delta < 0 && metric.Better < 0) ||
					(comparison.Delta > 0 && metric.Better > 0) ||
					comparison.Delta == 0 {
					continue
				}
				nameSplit := strings.Split(detail.BenchmarkName, util.PackageSeparator)
				ci := changeInfo{
					BenchmarkName: nameSplit[0] + util.PackageSeparator + truncateBenchmarkName(nameSplit[1], 32),
					PercentChange: fmt.Sprintf("%.2f%%", comparison.Delta),
				}
				if math.Abs(comparison.Delta) > highestPercentChange {
					highestPercentChange = math.Abs(comparison.Delta)
				}
				ci.ChangeSymbol = ":small_orange_diamond:"
				if math.Abs(comparison.Delta) > slackPercentageThreshold {
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

	s := newSlackClient(c.slackConfig.user, c.slackConfig.channel, c.slackConfig.token)
	return s.Post(
		slack.MsgOptionText(fmt.Sprintf("Microbenchmark comparison summary: %s", c.sheetDesc), false),
		slack.MsgOptionAttachments(attachments...),
	)
}

func (c *compare) compareUsingThreshold(comparisonResultsMap model.ComparisonResultsMap) error {
	var reportStrings []string

	for pkgName, comparisonResults := range comparisonResultsMap {
		var metrics []string

		for _, result := range comparisonResults {
			metricKey := result.Metric.Unit

			for _, detail := range result.Comparisons {
				comparison := detail.Comparison

				// If Delta is more negative than the threshold, then there's a concerning perf regression
				if (comparison.Delta*float64(result.Metric.Better))+c.threshold < 0 {
					metrics = append(metrics, fmt.Sprintf("Metric: %s, Benchmark: %s, Change: %s", metricKey, detail.BenchmarkName, comparison.FormattedDelta))
				}
			}
		}

		if len(metrics) > 0 {
			reportStrings = append(reportStrings, fmt.Sprintf("Package: %s\n%s", pkgName, strings.Join(metrics, "\n")))
		}
	}

	if len(reportStrings) > 0 {
		reportString := strings.Join(reportStrings, "\n\n")
		return errors.Errorf("there are benchmark regressions of > %.2f%% in the following packages:\n\n%s",
			c.threshold, reportString)
	}

	return nil
}

func (c *compare) createBenchSeries() ([]*benchseries.ComparisonSeries, error) {
	opts := benchseries.DefaultBuilderOptions()
	opts.Experiment = "run-time"
	opts.Compare = "cockroach"
	builder, err := benchseries.NewBuilder(opts)
	if err != nil {
		return nil, err
	}

	var benchBuf bytes.Buffer
	readFileFn := func(filePath string, required bool) error {
		data, err := os.ReadFile(filePath)
		if err != nil {
			if !required && oserror.IsNotExist(err) {
				return nil
			}
			return errors.Wrapf(err, "failed to read file %s", filePath)
		}
		benchBuf.Write(data)
		benchBuf.WriteString("\n")
		return nil
	}

	for k, v := range c.influxConfig.metadata {
		benchBuf.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}

	logPaths := map[string]string{
		"experiment": c.experimentDir,
		"baseline":   c.baselineDir,
	}
	for logType, dir := range logPaths {
		benchBuf.WriteString(fmt.Sprintf("cockroach: %s\n", logType))
		logPath := filepath.Join(dir, "metadata.log")
		if err = readFileFn(logPath, true); err != nil {
			return nil, err
		}
		for _, pkg := range c.packages {
			benchBuf.WriteString(fmt.Sprintf("pkg: %s\n", pkg))
			logPath = filepath.Join(dir, getReportLogName(reportLogName, pkg))
			if err = readFileFn(logPath, false); err != nil {
				return nil, err
			}
		}
	}

	benchReader := benchfmt.NewReader(bytes.NewReader(benchBuf.Bytes()), "buffer")
	recordsMap := make(map[string][]*benchfmt.Result)
	seen := make(map[string]map[string]struct{})
	for benchReader.Scan() {
		switch rec := benchReader.Result().(type) {
		case *benchfmt.SyntaxError:
			// In case the benchmark log is corrupted or contains a syntax error, we
			// want to return an error to the caller.
			return nil, fmt.Errorf("syntax error: %v", rec)
		case *benchfmt.Result:
			var cmp, pkg string
			for _, config := range rec.Config {
				if config.Key == "pkg" {
					pkg = string(config.Value)
				}
				if config.Key == opts.Compare {
					cmp = string(config.Value)
				}
			}
			key := pkg + util.PackageSeparator + string(rec.Name)
			// Update the name to include the package name. This is a workaround for
			// `benchseries`, that currently does not support package names.
			rec.Name = benchfmt.Name(key)
			recordsMap[key] = append(recordsMap[key], rec.Clone())
			// Determine if we've seen this package/benchmark combination for both
			// the baseline and experiment run.
			if _, ok := seen[key]; !ok {
				seen[key] = make(map[string]struct{})
			}
			seen[key][cmp] = struct{}{}
		}
	}

	// Add only the benchmarks that have been seen in both the baseline and
	// experiment run.
	for key, records := range recordsMap {
		if len(seen[key]) != 2 {
			continue
		}
		for _, rec := range records {
			builder.Add(rec)
		}
	}

	comparisons, err := builder.AllComparisonSeries(nil, benchseries.DUPE_REPLACE)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create comparison series")
	}
	return comparisons, nil
}

func (c *compare) pushToInfluxDB() error {
	client := influxdb2.NewClient(c.influxConfig.host, c.influxConfig.token)
	defer client.Close()
	writeAPI := client.WriteAPI("cockroach", "microbench")
	errorChan := writeAPI.Errors()

	comparisons, err := c.createBenchSeries()
	if err != nil {
		return err
	}

	for _, cs := range comparisons {
		cs.AddSummaries(0.95, 1000)
		residues := make(map[string]string)
		for _, r := range cs.Residues {
			residues[r.S] = r.Slice[0]
		}

		for idx, benchmarkName := range cs.Benchmarks {
			if len(cs.Summaries) == 0 {
				log.Printf("WARN: no summaries for %s", benchmarkName)
				continue
			}
			sum := cs.Summaries[0][idx]
			if !sum.Defined() {
				continue
			}

			experimentTime := cs.Series[0]
			ts, err := benchseries.ParseNormalizedDateString(experimentTime)
			if err != nil {
				return errors.Wrap(err, "error parsing experiment commit date")
			}
			fields := map[string]interface{}{
				"low":               sum.Low,
				"center":            sum.Center,
				"high":              sum.High,
				"upload-time":       residues["upload-time"],
				"baseline-commit":   cs.HashPairs[experimentTime].DenHash,
				"experiment-commit": cs.HashPairs[experimentTime].NumHash,
				"benchmarks-commit": residues["benchmarks-commit"],
			}
			pkg := strings.Split(benchmarkName, util.PackageSeparator)[0]
			benchmarkName = strings.Split(benchmarkName, util.PackageSeparator)[1]
			tags := map[string]string{
				"name":         benchmarkName,
				"unit":         cs.Unit,
				"pkg":          pkg,
				"repository":   "cockroach",
				"branch":       residues["branch"],
				"goarch":       residues["goarch"],
				"goos":         residues["goos"],
				"machine-type": residues["machine-type"],
			}
			p := influxdb2.NewPoint("benchmark-result", tags, fields, ts)
			writeAPI.WritePoint(p)
		}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		writeAPI.Flush()
	}()

	select {
	case err = <-errorChan:
		return errors.Wrap(err, "failed to write to InfluxDB")
	case <-done:
		return nil
	}
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
	return builder.AddMetrics(id, pkg+util.PackageSeparator, reader)
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
