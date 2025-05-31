// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
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
	for _, metric := range c.Benchmark.Metrics {
		metricName := metric.Name
		entry := c.MetricMap[metricName]
		if entry == nil {
			log.Printf("WARN: no metric found for benchmark metric %q", metricName)
			continue
		}
		benchmark := entry.BenchmarkEntries[c.EntryName]
		cc := entry.ComputeComparison(c.EntryName, string(Old), string(New))
		if cc == nil {
			log.Printf("WARN: no comparison found for benchmark metric %q:%q", c.EntryName, metricName)
			continue
		}
		status := statusTemplateFunc(c.status(metricName))
		oldSum := benchmark.Summaries[string(Old)]
		newSum := benchmark.Summaries[string(New)]
		summaryData = append(summaryData, SummaryData{
			Metric:    metricName,
			OldCenter: fmt.Sprintf("%s ±%s", formatValue(oldSum.Center, metricName), oldSum.PctRangeString()),
			NewCenter: fmt.Sprintf("%s ±%s", formatValue(newSum.Center, metricName), newSum.PctRangeString()),
			Delta:     cc.FormattedDelta,
			Note:      cc.Distribution.String(),
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

	jsonData, err := json.MarshalIndent(struct {
		Entries   []Entry
		Revisions Revisions
	}{
		Entries:   entries,
		Revisions: suite.Revisions,
	}, "", "  ")
	if err != nil {
		return err
	}

	formattedData := formatFloats(jsonData, 5)
	return os.WriteFile(path, formattedData, 0644)
}

// githubSummary creates a markdown summary of the comparison results.
func (c CompareResults) githubSummary() (string, error) {
	buf := bytes.NewBuffer(nil)
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
			return statusToDot(status)
		})
		data.BenchmarkStatus = statusToDot(finalStatus)
		data.Summaries = summaryData
		summaries = append(summaries, data)
	}

	tmpl, err := template.New("github").Parse(githubSummary)
	if err != nil {
		return "", err
	}
	err = tmpl.Execute(buf, struct {
		GitHubSummaryData []GitHubData
		Artifacts         map[Revision]string
		Description       string
		Commit            string
	}{
		GitHubSummaryData: summaries,
		Artifacts: map[Revision]string{
			Old: suite.artifactsURL(Old),
			New: suite.artifactsURL(New),
		},
		Commit: suite.revisionSHA(New),
	})
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func statusToDot(status Status) string {
	return string([]rune("⚪🟢🔴")[status])
}

// formatValue formats a value according to the unit of the metric.
func formatValue(val float64, metric string) string {
	cls := benchunit.ClassOf(metric)
	return benchunit.Scale(val, cls)
}

// formatFloats formats all floating point numbers in the JSON data to the given
// precision.
func formatFloats(jsonData []byte, precision int) []byte {
	re := regexp.MustCompile(`\d+\.\d+`)
	return re.ReplaceAllFunc(jsonData, func(match []byte) []byte {
		f, err := strconv.ParseFloat(string(match), 64)
		if err != nil {
			return match
		}
		return []byte(strconv.FormatFloat(f, 'f', precision, 64))
	})
}
