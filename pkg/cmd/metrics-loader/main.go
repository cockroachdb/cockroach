// Copyright 2024 The Cockroach Authors.
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
	"log"
	"os"

	"github.com/spf13/cobra"
)

func makeMetricsLoaderCommand() *cobra.Command {
	return &cobra.Command{
		Use:     "metrics-loader (flags)",
		Short:   "metrics-loader streams metrics from OpenMetrics files to a Prometheus remote-write endpoint.",
		Version: "v0.0",
		Long: `metrics-loader watches a Google Cloud Storage bucket for OpenMetrics format
files [1] and streams it to a Prometheus remote-write endpoint [2]. A file will
be moved to the completed directory if the file was successfully processed.

[1] https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md
[2] https://prometheus.io/docs/specs/remote_write_spec/
`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
}

func main() {
	cmd := makeMetricsLoaderCommand()
	if err := cmd.Execute(); err != nil {
		log.Printf("ERROR: %+v", err)
		os.Exit(1)
	}
}
