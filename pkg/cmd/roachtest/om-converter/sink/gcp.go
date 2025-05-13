// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sink

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

type gcpSink struct {
	ctx       context.Context
	client    *storage.Client
	bucket    *storage.BucketHandle
	directory string
	retryOpts retry.Options
}

func (g gcpSink) Sink(buffer *bytes.Buffer, filePath string, fileName string) error {
	finalFilePath := fmt.Sprintf("%s/%s/%s", g.directory, strings.TrimSuffix(filePath, "/stats.json"), fileName)
	object := g.bucket.Object(finalFilePath)

	// Use retry logic with exponential backoff for the write operation
	return retry.WithMaxAttempts(g.ctx, g.retryOpts, g.retryOpts.MaxRetries+1, func() error {
		// Create a new writer for each attempt
		writer := object.NewWriter(g.ctx)

		// Write the buffer to the GCS object
		_, err := writer.Write(buffer.Bytes())
		if err != nil {
			// Close the writer before returning the error
			closeErr := writer.Close()
			if closeErr != nil {
				fmt.Printf("%v\n", errors.Wrap(closeErr, "failed to close writer after write error"))
			}
			return fmt.Errorf("failed to write to GCS object: %w", err)
		}

		// Close the writer after successful write
		err = writer.Close()
		if err != nil {
			return fmt.Errorf("failed to close writer: %w", err)
		}

		return nil
	})
}

func (g gcpSink) Close() {
	err := g.client.Close()
	if err != nil {
		fmt.Printf("%v\n", errors.Wrap(err, "failed to close client"))
	}
}

func NewGCPSink(ctx context.Context, bucketPath, directory string) (Sink, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client: %w", err)
	}

	bucket := client.Bucket(bucketPath)

	// Configure retry options with exponential backoff
	retryOpts := retry.Options{
		InitialBackoff:      100 * time.Millisecond,
		MaxBackoff:          2 * time.Second,
		Multiplier:          2.0,
		MaxRetries:          5,
		RandomizationFactor: 0.15,
	}

	return &gcpSink{
		ctx:       ctx,
		bucket:    bucket,
		client:    client,
		directory: directory,
		retryOpts: retryOpts,
	}, nil
}
