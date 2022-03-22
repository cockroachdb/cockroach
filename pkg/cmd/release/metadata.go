// Copyright 2019 The Cockroach Authors.
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
	"encoding/json"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
)

type buildInfo struct {
	Tag       string `json:"tag"`
	SHA       string `json:"sha"`
	Timestamp string `json:"timestamp"`
}

// getBuildInfo retrieves the release qualification metadata file and returns its unmarshalled struct
func getBuildInfo(ctx context.Context, bucket string, obj string) (buildInfo, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return buildInfo{}, fmt.Errorf("cannot create GCS client: %w", err)
	}
	reader, err := client.Bucket(bucket).Object(obj).NewReader(ctx)
	if err != nil {
		return buildInfo{}, fmt.Errorf("cannot create GCS reader: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return buildInfo{}, fmt.Errorf("cannot read GCS object: %w", err)
	}
	var info buildInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return buildInfo{}, fmt.Errorf("cannot unmarshal metadata: %w", err)
	}
	return info, nil
}

// publishReleaseCandidateInfo copies release candidate metadata to a separate location.
// This file will be used by the week 1 automation.
func publishReleaseCandidateInfo(
	ctx context.Context, next releaseInfo, bucket string, obj string,
) error {
	marshalled, err := json.MarshalIndent(next.buildInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("cannot marshall buildInfo: %w", err)
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("cannot create storage client: %w", err)
	}
	wc := client.Bucket(bucket).Object(obj).NewWriter(ctx)
	wc.ContentType = "application/json"
	if _, err := wc.Write(marshalled); err != nil {
		return fmt.Errorf("cannot write to bucket: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("cannot close storage writer filehandle: %w", err)
	}
	return nil
}
