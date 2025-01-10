// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/util/sparkline"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/fatih/color"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

const (
	clusterQueryTemplate = "sum(rate(txn_commits{cluster=~\"$CLUSTERS\"}[5m])) by (cluster)"

	clusterHdr     = "Cluster"
	cloudsHdr      = "Clouds"
	sizeHdr        = "Size"
	vmHdr          = "VM"
	archHdr        = "Arch"
	costPerHourHdr = "$/hour"
	costSpentHdr   = "$ Spent"
	costPerTTLHdr  = "$/TTL"
	uptimeHdr      = "Uptime"
	ttlHdr         = "TTL"
	roachtestHdr   = "Roachtest"
	txnCommitsHdr  = "Txn_Commits"
)

var (
	allHeaders = [12]string{clusterHdr, cloudsHdr, sizeHdr, vmHdr, archHdr, costPerHourHdr, costSpentHdr, costPerTTLHdr, uptimeHdr, ttlHdr, roachtestHdr, txnCommitsHdr}
)

func clusterToSparkline(clusterNames []string) (map[string]string, error) {
	res := make(map[string]string)
	query := strings.ReplaceAll(clusterQueryTemplate, "$CLUSTERS", strings.Join(clusterNames, "|"))
	from := timeutil.Now().Add(time.Duration(-60) * time.Minute)
	to := timeutil.Now()

	promClient, err := prometheus.NewPromClient(context.Background(), prometheus.DefaultPromHostUrl, true)
	if err != nil {
		return res, err
	}
	m, err := prometheus.QueryRange(context.Background(), promClient, query, from, to, prometheus.DefaultScrapeInterval)
	if err != nil {
		return res, err
	}
	var errs error
	for _, labelValToSeries := range m {
		for clusterName, series := range labelValToSeries {
			points := prometheus.Values(series)
			img, err := sparkline.DrawSparkline(points, 250, 35)
			if img != nil && err == nil {
				if s, err := sparkline.ITermEncodePNGToString(img, ""); err != nil {
					errs = errors.CombineErrors(errs, err)
				} else {
					res[clusterName] = s
				}
			}
		}
	}
	return res, errs
}

func listDefault(cloud cloud.Cloud, names []string, maxClusterName int) error {
	var res error
	costPrinter := message.NewPrinter(language.English)
	totalCostPerHour := 0.0
	colorByCostBucket := func(cost float64) func(string, ...interface{}) string {
		switch {
		case cost <= 100:
			return color.HiGreenString
		case cost <= 1000:
			return color.HiBlueString
		default:
			return color.HiRedString
		}
	}
	machineType := func(clusterVMs vm.List) string {
		return clusterVMs[0].MachineType
	}
	cpuArch := func(clusterVMs vm.List) string {
		// Display CPU architecture and family.
		if clusterVMs[0].CPUArch == "" {
			// N.B. Either a local cluster or unsupported cloud provider.
			return ""
		}
		if clusterVMs[0].CPUFamily != "" {
			return clusterVMs[0].CPUFamily
		}
		if clusterVMs[0].CPUArch != vm.ArchAMD64 {
			return string(clusterVMs[0].CPUArch)
		}
		// AMD64 is the default, so don't display it.
		return ""
	}
	// Max test name is dictated by label length (63); for brevity, we truncate it.
	// https://cloud.google.com/resource-manager/docs/labels-overview#requirements
	shortTestName := func(testName string) string {
		if len(testName) > 32 {
			return testName[:32]
		}
		return testName
	}
	var sparks map[string]string
	if listSparkline {
		var err error
		sparks, err = clusterToSparkline(names)
		if err != nil {
			res = errors.CombineErrors(res, err)
		}
	}
	// Align columns right and separate with at least two spaces.
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', tabwriter.AlignRight)
	// N.B. colors use escape codes which don't play nice with tabwriter [1].
	// We use a hacky workaround below to color the empty string.
	// [1] https://github.com/golang/go/issues/12073

	printHeaders(tw, maxClusterName)
	// Now print one row per cluster.
	for _, name := range names {
		c := cloud.Clusters[name]
		// Suppress default GCE project name for brevity.
		clouds := strings.ReplaceAll(strings.Join(c.Clouds(), ""), fmt.Sprintf("gce:%s", gce.DefaultProject()), "gce")

		// N.B. Tabwriter doesn't support per-column alignment. It looks odd to have the cluster names right-aligned,
		// so we make it left-aligned.
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s", name+strings.Repeat(" ", maxClusterName-len(name)), clouds,
			len(c.VMs), machineType(c.VMs), cpuArch(c.VMs))

		if c.IsLocal() {
			//N.B. for proper formatting, we have to use colored strings since those emit terminal escape codes even for the empty string.
			fmt.Fprintf(tw, "\t%s\t%s\t%s\t%s\t%s\t%s\t", color.HiWhiteString(""), color.HiWhiteString(""),
				color.HiWhiteString(""), "", color.HiWhiteString(""), color.HiWhiteString(""))
			if listSparkline {
				fmt.Fprintf(tw, "\t%s", strings.Repeat(" ", 15))
			}
			fmt.Fprintf(tw, "\n")
			continue
		}
		timeRemaining := c.LifetimeRemaining().Round(time.Second)
		alive := timeutil.Since(c.CreatedAt).Round(time.Minute)
		testName := c.VMs[0].Labels["test_name"]
		if listCost {
			cost := c.CostPerHour
			totalCostPerHour += cost
			costSinceCreation := cost * float64(alive) / float64(time.Hour)
			costRemaining := cost * float64(timeRemaining) / float64(time.Hour)

			fmt.Fprintf(tw, "\t%s\t%s\t%s\t%s\t%s\t%s",
				color.HiGreenString(costPrinter.Sprintf("$%.2f", cost)),
				colorByCostBucket(costSinceCreation)(costPrinter.Sprintf("$%.2f", costSinceCreation)),
				colorByCostBucket(costRemaining)(costPrinter.Sprintf("$%.2f", costRemaining)),
				color.HiWhiteString(alive.String()),
				color.HiWhiteString(timeRemaining.String()),
				color.HiWhiteString(shortTestName(testName)))
		} else {
			fmt.Fprintf(tw, "\t%s\t%s\t%s",
				color.HiWhiteString(alive.String()),
				color.HiWhiteString(timeRemaining.String()),
				color.HiWhiteString(shortTestName(testName)))
		}
		if listSparkline {
			s := sparks[name]
			if s == "" {
				fmt.Fprintf(tw, "\t%s", strings.Repeat(" ", 15))
			} else {
				fmt.Fprintf(tw, "\t%s", s)
			}
		} else {
			// N.B. We need this to compensate for the leading \t in the column header.
			fmt.Fprintf(tw, "\t")
		}
		fmt.Fprintf(tw, "\n")
	}

	if err := tw.Flush(); err != nil {
		res = errors.CombineErrors(res, err)
	}
	if totalCostPerHour > 0 {
		_, _ = costPrinter.Printf("\nTotal cost per hour: $%.2f\n", totalCostPerHour)
	}

	return res
}

func printHeaders(tw *tabwriter.Writer, maxClusterName int) {
	headers := make([]string, len(allHeaders))
	copy(headers, allHeaders[:])
	header2color := make(map[string]func(string, ...interface{}) string)
	header2color[costPerHourHdr] = color.HiWhiteString
	header2color[costSpentHdr] = color.HiWhiteString
	header2color[costPerTTLHdr] = color.HiWhiteString
	header2color[uptimeHdr] = color.HiWhiteString
	header2color[ttlHdr] = color.HiWhiteString
	header2color[roachtestHdr] = color.HiWhiteString
	if !listCost {
		// remove cost related headers
		headers = append(headers[:5], headers[8:]...)
	}
	if !listSparkline {
		// remove sparkline header
		headers = headers[:len(headers)-1]
	}
	for i := 0; i < len(headers); i++ {
		h := headers[i]
		if i == 0 {
			// Left align the first column.
			h = h + strings.Repeat(" ", maxClusterName-len(h))
		}
		if color, ok := header2color[h]; ok {
			fmt.Fprintf(tw, "%s\t", color(h))
		} else {
			fmt.Fprintf(tw, "%s\t", h)
		}
		if i == len(headers)-1 {
			fmt.Fprintf(tw, "\n")
		}
	}
	// Print separator.
	for i := 0; i < len(headers); i++ {
		h := headers[i]
		if color, ok := header2color[h]; ok {
			fmt.Fprintf(tw, "%s\t", color(""))
		} else {
			fmt.Fprintf(tw, "%s\t", "")
		}
		if i == len(headers)-1 {
			fmt.Fprintf(tw, "\n")
		}
	}
}
