// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
)

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
	service, err := google.New(ctx)
	if err != nil {
		return nil, err
	}
	return &compare{compareConfig: config, service: service, packages: packages, ctx: ctx}, nil
}

func defaultCompareConfig() compareConfig {
	return compareConfig{
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

func (c *compare) publishToGoogleSheets(
	metricMaps map[string]*model.MetricMap,
) (map[string]string, error) {
	sheets := make(map[string]string)
	for pkgGroup, metricMap := range metricMaps {
		sheetName := pkgGroup + "/..."
		if c.sheetDesc != "" {
			sheetName = fmt.Sprintf("%s (%s)", sheetName, c.sheetDesc)
		}
		url, err := c.service.CreateSheet(c.ctx, sheetName, *metricMap, "old", "new")
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
