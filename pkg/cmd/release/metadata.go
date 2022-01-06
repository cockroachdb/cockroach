package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
)

type metadata struct {
	Tag       string `json:"tag"`
	SHA       string `json:"sha"`
	Timestamp string `json:"timestamp"`
}

func (m metadata) marshall() ([]byte, error) {
	ret, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func fetchVersionJSON(ctx context.Context, bucket string, obj string) (metadata, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return metadata{}, err
	}
	reader, err := client.Bucket(bucket).Object(obj).NewReader(ctx)
	if err != nil {
		return metadata{}, err
	}
	defer reader.Close()

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return metadata{}, err
	}
	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return metadata{}, err
	}
	return meta, nil
}

func publishJSON(ctx context.Context, next release, bucket string, obj string) error {
	out, err := next.nextReleaseMetadata.marshall()
	if err != nil {
		return fmt.Errorf("cannot marshall metadata: %w", err)
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("cannot create storage client: %w", err)
	}
	wc := client.Bucket(bucket).Object(obj).NewWriter(ctx)
	wc.ContentType = "application/json"
	if _, err := wc.Write(out); err != nil {
		return fmt.Errorf("cannot write to bucket: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("cannot close storage writer filehandle: %w", err)
	}
	return nil
}
