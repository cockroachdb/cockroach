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
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/spf13/cobra"
)

func makeMetricsLoaderCommand() *cobra.Command {
	config := GoogleWatcherConfig{
		TopicID:             "metrics-loader",
		SubscriptionID:      "metrics-loader-sub",
		ProjectID:           "cockroach-testeng-infra",
		BucketName:          "cockroach-testeng-metrics",
		BucketWatchPath:     "metrics-loader/incoming",
		BucketCompletedPath: "metrics-loader/processed",
	}
	senderURL := "http://localhost:9201/write"
	cmd := &cobra.Command{
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
		RunE: func(cmd *cobra.Command, args []string) error {
			sender := NewHTTPSender(senderURL)
			processor := NewProcessor(NewOpenMetricsReader(), sender)

			ctx, cancel := context.WithCancel(context.Background())
			watcher, err := NewGoogleWatcher(ctx, processor, config)
			if err != nil {
				cancel()
				return err
			}

			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err = watcher.Start()
				cancel()
			}()

			sigCh := make(chan os.Signal, 1)
			defer close(sigCh)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
			select {
			case <-sigCh:
				log.Println("Received SIGTERM, exiting gracefully...")
				cancel()
			case <-ctx.Done():
				log.Println("Received cancellation, exiting gracefully...")
			}
			wg.Wait()
			return err
		},
	}
	cmd.Flags().StringVar(&config.ProjectID, "project-id", config.ProjectID, "Google Cloud project ID")
	cmd.Flags().StringVar(&config.BucketName, "bucket-name", config.BucketName, "Google Cloud Storage bucket name")
	cmd.Flags().StringVar(&config.BucketWatchPath, "bucket-watch-path", config.BucketWatchPath, "Google Cloud Storage bucket path to watch for incoming metrics")
	cmd.Flags().StringVar(&config.BucketCompletedPath, "bucket-completed-path", config.BucketCompletedPath, "Google Cloud Storage bucket path to move processed metrics")
	cmd.Flags().StringVar(&config.TopicID, "topic-id", config.TopicID, "Google Cloud Pub/Sub topic ID")
	cmd.Flags().StringVar(&config.SubscriptionID, "subscription-id", config.SubscriptionID, "Google Cloud Pub/Sub subscription ID")
	cmd.Flags().StringVar(&senderURL, "write-url", senderURL, "Prometheus remote-write URL")
	return cmd
}

func main() {
	cmd := makeMetricsLoaderCommand()
	if err := cmd.Execute(); err != nil {
		log.Printf("ERROR: %+v", err)
		os.Exit(1)
	}
}
