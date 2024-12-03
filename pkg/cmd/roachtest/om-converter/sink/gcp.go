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

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
)

type gcpSink struct {
	ctx       context.Context
	client    *storage.Client
	bucket    *storage.BucketHandle
	directory string
}

func (g gcpSink) Sink(buffer *bytes.Buffer, filePath string, fileName string) error {
	finalFilePath := fmt.Sprintf("%s/%s/%s", g.directory, strings.TrimSuffix(filePath, "/stats.json"), fileName)
	object := g.bucket.Object(finalFilePath)
	writer := object.NewWriter(g.ctx)
	defer func() {
		err := writer.Close()
		if err != nil {
			fmt.Printf("%v\n", errors.Wrap(err, "failed to close writer"))
		}
	}()

	// Write the buffer to the GCS object
	_, err := writer.Write(buffer.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write to GCS object: %w", err)
	}

	//fmt.Printf("File Synced %s\n", finalFilePath)
	return nil
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
	return &gcpSink{
		ctx:       ctx,
		bucket:    bucket,
		client:    client,
		directory: directory,
	}, nil
}
