// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"text/template"

	"golang.org/x/exp/maps"
	"golang.org/x/perf/benchmath"
	"golang.org/x/perf/benchunit"
)

//go:embed template/github_summary.md
var githubSummary string

type SummaryData struct {
	Metric    string
	OldCenter string
	NewCenter string
	Delta     string
	Note      string
	Threshold string
	Status    string
}

type GitHubData struct {
	BenchmarkStatus string
	DisplayName     string
	Labels          string
	Summaries       []SummaryData
	Benchdiff       BenchdiffData
}

type BenchdiffData struct {
	Package    string
	Run        string
	Dir        map[Revision]string
	BinURL     map[Revision]string
	BinDest    map[Revision]string
	TrimmedSHA map[Revision]string
	Old        Revision
	New        Revision
}

func (c *CompareResult) generateSummaryData(
	statusTemplateFunc func(status Status) string,
) []SummaryData {
	summaryData := make([]SummaryData, 0, len(c.MetricMap))
	for metricName, entry := range c.MetricMap {
		benchmark := entry.BenchmarkEntries[c.EntryName]
		cc := entry.ComputeComparison(c.EntryName, string(Old), string(New))
		if cc == nil {
			log.Printf("WARN: no comparison found for benchmark metric %q:%q", c.EntryName, metricName)
			continue
		}
		threshold := c.Benchmark.Thresholds[metricName] * 100.0
		status := statusTemplateFunc(c.status(metricName))
		oldSum := benchmark.Summaries[string(Old)]
		newSum := benchmark.Summaries[string(New)]
		summaryData = append(summaryData, SummaryData{
			Metric:    metricName,
			OldCenter: fmt.Sprintf("%s ±%s", formatValue(oldSum.Center, metricName), oldSum.PctRangeString()),
			NewCenter: fmt.Sprintf("%s ±%s", formatValue(newSum.Center, metricName), newSum.PctRangeString()),
			Delta:     cc.FormattedDelta,
			Note:      cc.Distribution.String(),
			Threshold: fmt.Sprintf("%.1f%%", threshold),
			Status:    status,
		})
	}
	sort.Slice(summaryData, func(i, j int) bool {
		return summaryData[i].Metric > summaryData[j].Metric
	})
	return summaryData
}

func (c *CompareResult) benchdiffData() BenchdiffData {
	d := BenchdiffData{
		Package:    c.Benchmark.Package,
		Run:        c.Benchmark.Name,
		Dir:        make(map[Revision]string),
		BinURL:     make(map[Revision]string),
		BinDest:    make(map[Revision]string),
		TrimmedSHA: make(map[Revision]string),
		Old:        Old,
		New:        New,
	}
	for _, revision := range []Revision{Old, New} {
		sha := suite.revisionSHA(revision)
		shortSHA := sha[:int(math.Min(float64(len(sha)), 7))]
		d.Dir[revision] = fmt.Sprintf("benchdiff/%s/bin/%s",
			shortSHA, c.Benchmark.packageHash())
		d.BinURL[revision] = suite.binURL(revision, c.Benchmark)
		d.BinDest[revision] = fmt.Sprintf("%s/%s", d.Dir[revision],
			"cockroachdb_cockroach_"+c.Benchmark.sanitizedPackageName())
		d.TrimmedSHA[revision] = shortSHA
	}

	return d
}

// writeJSONSummary writes a JSON summary of the comparison results to the given
// path.
func (c CompareResults) writeJSONSummary(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	type (
		Data struct {
			Metric  string
			Summary benchmath.Summary
			Sample  benchmath.Sample
		}
		Entry struct {
			Name       string
			Count      int
			Iterations int
			Data       map[string][]Data
		}
	)

	entries := make([]Entry, len(c))
	for idx, cr := range c {
		data := make(map[string][]Data)
		metricKeys := maps.Keys(cr.MetricMap)
		sort.Strings(metricKeys)
		for _, name := range metricKeys {
			m := cr.MetricMap[name]
			for _, r := range []Revision{Old, New} {
				if m.BenchmarkEntries[cr.EntryName].Summaries[string(r)] == nil {
					continue
				}
				if m.BenchmarkEntries[cr.EntryName].Samples[string(r)] == nil {
					continue
				}
				data[string(r)] = append(data[string(r)], Data{
					Metric:  name,
					Summary: *m.BenchmarkEntries[cr.EntryName].Summaries[string(r)],
					Sample:  *m.BenchmarkEntries[cr.EntryName].Samples[string(r)],
				})
			}
		}
		entries[idx] = Entry{
			Name:       cr.Benchmark.Name,
			Count:      cr.Benchmark.Count,
			Iterations: cr.Benchmark.Iterations,
			Data:       data,
		}
	}
	return encoder.Encode(struct {
		Entries   []Entry
		Revisions Revisions
	}{
		Entries:   entries,
		Revisions: suite.Revisions,
	})
}

// writeGitHubSummary writes a markdown summary of the comparison results to the
// given path.
func (c CompareResults) writeGitHubSummary(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	regressionDetected := false
	summaries := make([]GitHubData, 0, len(c))
	for _, cr := range c {
		finalStatus := NoChange
		data := GitHubData{
			DisplayName: cr.Benchmark.DisplayName,
			Labels:      strings.Join(cr.Benchmark.Labels, ", "),
			Benchdiff:   cr.benchdiffData(),
		}
		summaryData := cr.generateSummaryData(func(status Status) string {
			if status > finalStatus {
				finalStatus = status
			}
			if status == Regression {
				regressionDetected = true
			}
			return statusToDot(status)
		})
		data.BenchmarkStatus = statusToDot(finalStatus)
		data.Summaries = summaryData
		summaries = append(summaries, data)
	}

	tmpl, err := template.New("github").Parse(githubSummary)
	if err != nil {
		return err
	}
	description := "No regressions detected!"
	if regressionDetected {
		description = "A regression has been detected, please investigate further!"
	}
	return tmpl.Execute(file, struct {
		GitHubSummaryData []GitHubData
		Artifacts         map[Revision]string
		Description       string
		Commit            string
	}{
		GitHubSummaryData: summaries,
		Description:       description,
		Artifacts: map[Revision]string{
			Old: suite.artifactsURL(Old),
			New: suite.artifactsURL(New),
		},
		Commit: suite.revisionSHA(New),
	})
}

func statusToDot(status Status) string {
	return string([]rune("⚪🟢🟡🔴")[status])
}

// formatValue formats a value according to the unit of the metric.
func formatValue(val float64, metric string) string {
	cls := benchunit.ClassOf(metric)
	return benchunit.Scale(val, cls)
}
