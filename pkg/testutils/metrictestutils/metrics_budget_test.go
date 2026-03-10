// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrictestutils_test

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/internal/reporoot"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// metricInfo mirrors the MetricInfo struct in pkg/cli/gen.go for
// YAML parsing. Only the fields needed for budget checking are
// included.
type metricInfo struct {
	ExportedName string `yaml:"exported_name"`
	Visibility   string `yaml:"visibility,omitempty"`
	Owner        string `yaml:"owner"`
}

type category struct {
	Metrics []metricInfo
}

type layer struct {
	Categories []category
}

type yamlOutput struct {
	Layers []layer
}

// loadMetricsYAML reads docs/generated/metrics/metrics.yaml and
// returns each metric's exported name and owner.
func loadMetricsYAML(t *testing.T) []metricInfo {
	t.Helper()

	var path string
	if bazel.BuiltWithBazel() {
		var err error
		path, err = bazel.Runfile("docs/generated/metrics/metrics.yaml")
		require.NoError(t, err, "failed to find metrics.yaml in runfiles")
	} else {
		repoRoot := reporoot.Get()
		require.NotEmpty(t, repoRoot, "could not find repository root")
		path = repoRoot + "/docs/generated/metrics/metrics.yaml"
	}

	data, err := os.ReadFile(path)
	require.NoError(t, err, "failed to read metrics.yaml; run `./dev generate` to generate it")

	var output yamlOutput
	require.NoError(t, yaml.Unmarshal(data, &output), "failed to parse metrics.yaml")

	var metrics []metricInfo
	for _, l := range output.Layers {
		for _, c := range l.Categories {
			metrics = append(metrics, c.Metrics...)
		}
	}
	require.Greater(t, len(metrics), 0, "no metrics found in metrics.yaml")
	return metrics
}

// teamBudgets defines the maximum number of metrics each CODEOWNERS
// team is allowed to own. Adding metrics beyond the budget requires
// updating this map with justification in the commit message.
//
// To update after adding metrics: run `./dev generate` to refresh
// metrics.yaml, then run this test, read the failure message showing
// the new count, and update the budget here.
var teamBudgets = map[string]int{
	"cockroachdb/kv":                   1100,
	"cockroachdb/admission-control":    475,
	"cockroachdb/jobs":                 425,
	"cockroachdb/sql-queries":          275,
	"cockroachdb/cdc":                  130,
	"cockroachdb/obs-prs":              100,
	"cockroachdb/sql-foundations":      65,
	"cockroachdb/server":               45,
	"cockroachdb/security-engineering": 25,
	"cockroachdb/disaster-recovery":    20,
	"cockroachdb/unowned":              17,
}

// essentialBudgets defines the maximum number of ESSENTIAL-visibility
// metrics each CODEOWNERS team is allowed to own. Essential metrics
// are those critical for monitoring cluster health. Adding essential
// metrics beyond the budget requires updating this map with
// justification in the commit message.
var essentialBudgets = map[string]int{
	"cockroachdb/kv":                45,
	"cockroachdb/sql-queries":       21,
	"cockroachdb/sql-foundations":   19,
	"cockroachdb/jobs":              17,
	"cockroachdb/obs-prs":           15,
	"cockroachdb/cdc":               11,
	"cockroachdb/unowned":           2,
	"cockroachdb/admission-control": 2,
	"cockroachdb/disaster-recovery": 1,
}

// supportBudgets defines the maximum number of SUPPORT-visibility
// metrics each CODEOWNERS team is allowed to own. Support metrics
// are used to diagnose production issues and are included in
// tsdump output. Adding support metrics beyond the budget requires
// updating this map with justification in the commit message.
var supportBudgets = map[string]int{
	"cockroachdb/kv":                56,
	"cockroachdb/obs-prs":           13,
	"cockroachdb/sql-queries":       2,
	"cockroachdb/cdc":               2,
	"cockroachdb/admission-control": 1,
}

// checkBudgets verifies that team metric counts stay within the given
// budgets. It logs a summary table and fails the test for any team
// that exceeds its budget or has no budget entry.
func checkBudgets(t *testing.T, label string, teamCounts map[string]int, budgets map[string]int) {
	t.Helper()

	type teamEntry struct {
		name   string
		count  int
		budget int
	}
	entries := make([]teamEntry, 0, len(teamCounts))
	for name, count := range teamCounts {
		entries = append(entries, teamEntry{
			name:   name,
			count:  count,
			budget: budgets[name],
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].count > entries[j].count
	})

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "\n%s metrics:\n", label)
	fmt.Fprintf(&buf, "%-40s %8s %8s\n", "Team", "Count", "Budget")
	fmt.Fprintf(&buf, "%s %s %s\n",
		strings.Repeat("-", 40), strings.Repeat("-", 8), strings.Repeat("-", 8))

	total := 0
	for _, e := range entries {
		marker := ""
		if e.budget > 0 && e.count > e.budget {
			marker = " OVER"
		}
		fmt.Fprintf(&buf, "%-40s %8d %8d%s\n", e.name, e.count, e.budget, marker)
		total += e.count
	}
	fmt.Fprintf(&buf, "%s %s %s\n",
		strings.Repeat("-", 40), strings.Repeat("-", 8), strings.Repeat("-", 8))
	fmt.Fprintf(&buf, "%-40s %8d\n", "TOTAL", total)
	t.Log(buf.String())

	for _, e := range entries {
		if _, ok := budgets[e.name]; !ok {
			t.Errorf("team %q has %d %s metrics but no budget defined; add an entry",
				e.name, e.count, label)
			continue
		}
		if e.count > e.budget {
			t.Errorf("team %q has %d %s metrics, exceeding budget of %d",
				e.name, e.count, label, e.budget)
		}
	}
}

// TestMetricsInBudget reads docs/generated/metrics/metrics.yaml
// (produced by `cockroach gen metric-list`) which contains the
// owning team and visibility for each metric, and verifies that
// every team stays within its metric budgets. Three budgets are
// enforced: total metrics per team (All), essential-visibility
// metrics per team (Essential), and support-visibility metrics
// per team (Support). This prevents unbounded metric growth by
// requiring explicit budget increases when new metrics are added.
func TestMetricsInBudget(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	metrics := loadMetricsYAML(t)

	allCounts := make(map[string]int)
	essentialCounts := make(map[string]int)
	supportCounts := make(map[string]int)
	var unattributed []string
	for _, m := range metrics {
		if m.Owner == "" {
			unattributed = append(unattributed, m.ExportedName)
			continue
		}
		allCounts[m.Owner]++
		switch m.Visibility {
		case "ESSENTIAL":
			essentialCounts[m.Owner]++
		case "SUPPORT":
			supportCounts[m.Owner]++
		}
	}
	sort.Strings(unattributed)

	if len(unattributed) > 0 {
		t.Errorf("metrics with no owner in metrics.yaml (%d); re-run `./dev generate`:",
			len(unattributed))
		for _, name := range unattributed {
			t.Errorf("  %s", name)
		}
	}

	t.Run("All", func(t *testing.T) {
		checkBudgets(t, "All", allCounts, teamBudgets)
	})

	t.Run("Essential", func(t *testing.T) {
		checkBudgets(t, "Essential", essentialCounts, essentialBudgets)
	})

	t.Run("Support", func(t *testing.T) {
		checkBudgets(t, "Support", supportCounts, supportBudgets)
	})
}
