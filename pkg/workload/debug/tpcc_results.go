// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package debug

import (
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var tpccMergeResultsCmd = &cobra.Command{
	Use: "tpcc-merge-results [<hist-file> [<hist-file>...]]",
	Short: "tpcc-merge-results merges the histograms from parallel runs of " +
		"TPC-C to compute a combined result.",
	RunE: tpccMergeResults,
	Args: cobra.MinimumNArgs(1),
}

func init() {
	flags := tpccMergeResultsCmd.Flags()
	flags.Int("warehouses", 0, "number of aggregate warehouses in all of the histograms")
}

func tpccMergeResults(cmd *cobra.Command, args []string) error {
	// We want to take histograms and merge them
	warehouses, err := cmd.Flags().GetInt("warehouses")
	if err != nil {
		return errors.Wrap(err, "no warehouses flag found")
	}
	var results []*tpcc.Result

	for _, fname := range args {
		snapshots, err := histogram.DecodeSnapshots(fname)
		if err != nil {
			return errors.Wrapf(err, "failed to decode histograms at %q", fname)
		}
		results = append(results, tpcc.NewResultWithSnapshots(warehouses, 0, snapshots))
	}

	res := tpcc.MergeResults(results...)
	out := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(out, "Duration: %.5v, Warehouses: %v, Efficiency: %.4v, tpmC: %.2f\n",
		res.Elapsed, res.ActiveWarehouses, res.Efficiency(), res.TpmC())
	_, _ = fmt.Fprintf(out, "_elapsed___ops/sec(cum)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)\n")

	var queries []string
	for query := range res.Cumulative {
		queries = append(queries, query)
	}
	sort.Strings(queries)
	for _, query := range queries {
		hist := res.Cumulative[query]
		_, _ = fmt.Fprintf(out, "%7.1fs %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f %s\n",
			res.Elapsed.Seconds(),
			float64(hist.TotalCount())/res.Elapsed.Seconds(),
			time.Duration(hist.ValueAtQuantile(50)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(90)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(95)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(99)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(100)).Seconds()*1000,
			query,
		)
	}
	return nil
}
