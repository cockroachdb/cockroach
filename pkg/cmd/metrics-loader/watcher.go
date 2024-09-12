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
	"encoding/json"
	"fmt"
	"log"
	"path"
	"path/filepath"
	"strings"

	"cloud.google.com/go/pubsub"
	"github.com/cockroachdb/errors"
)

type GoogleWatcherConfig struct {
	ProjectID           string
	TopicID             string
	BucketName          string
	BucketWatchPath     string
	BucketCompletedPath string
	SubscriptionID      string
}

type GoogleWatcher struct {
	GoogleWatcherConfig
	processor     Processor
	googleStorage GoogleStorage
	ctx           context.Context
}

type StorageData struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`
}

// NewGoogleWatcher creates a new GoogleWatcher that watches a bucket for
// OpenMetrics files, with the `om` extension, and processes it using the
// provided Processor.
func NewGoogleWatcher(
	ctx context.Context, processor Processor, config GoogleWatcherConfig,
) (*GoogleWatcher, error) {
	storage, err := NewGoogleStorage()
	if err != nil {
		return nil, err
	}
	return &GoogleWatcher{
		GoogleWatcherConfig: config,
		processor:           processor,
		googleStorage:       storage,
		ctx:                 ctx,
	}, nil
}

func (w *GoogleWatcher) Start() error {
	log.Print("Starting watcher")
	// Set up Google Cloud Pub/Sub client.
	pubsubClient, err := pubsub.NewClient(w.ctx, w.ProjectID)
	if err != nil {
		return errors.Wrap(err, "failed to create pubsub client")
	}

	// Delete Google Cloud Pub/Sub topic if it already exists.
	topic := pubsubClient.Topic(w.TopicID)
	exists, err := topic.Exists(w.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to check if pubsub topic exists")
	}
	if !exists {
		// Set up Google Cloud Pub/Sub topic.
		topic, err = pubsubClient.CreateTopic(w.ctx, w.TopicID)
		if err != nil {
			return errors.Wrap(err, "failed to create pubsub topic")
		}
	}

	// Set up Google Cloud Storage bucket notification.
	err = w.googleStorage.RegisterFinalizeNotification(w.BucketName, w.ProjectID, w.TopicID)
	if err != nil {
		return errors.Wrap(err, "failed to register bucket notification")
	}

	sub := pubsubClient.Subscription(w.SubscriptionID)
	exists, err = sub.Exists(w.ctx)
	if err != nil {
		return errors.Wrap(err, "failed to check if pubsub subscription exists")
	}
	if !exists {
		// Subscribe to Google Cloud Pub/Sub subscription.
		sub, err = pubsubClient.CreateSubscription(w.ctx, w.SubscriptionID, pubsub.SubscriptionConfig{
			Topic: topic,
		})
		if err != nil {
			return errors.Wrap(err, "failed to create pubsub subscription")
		}
	}
	log.Printf("Watcher started for subscription %q", sub.String())

	return sub.Receive(w.ctx, func(ctx context.Context, msg *pubsub.Message) {
		defer msg.Ack()

		var data StorageData
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			log.Printf("Failed to unmarshal event data: %v", err)
			return
		}

		// Only process files in the configured bucket path with the `.om`
		// extension.
		dir := path.Dir(data.Name)
		if dir != w.BucketWatchPath {
			return
		}
		if !strings.HasSuffix(data.Name, ".om") {
			return
		}

		log.Printf("Received bucket file event %q", data.Name)
		uri := fmt.Sprintf("gs://%s/%s", data.Bucket, data.Name)
		if err := w.processFile(uri); err != nil {
			log.Printf("Failed to process file %q: %v", uri, err)
		}
		log.Printf("Finished processing file %q", uri)

	})
}

func (w *GoogleWatcher) processFile(uri string) error {
	reader, err := w.googleStorage.Read(uri)
	if err != nil {
		return err
	}
	defer reader.Close()
	err = w.processor.Process(reader)
	if err != nil {
		return err
	}
	return w.googleStorage.Move(uri, w.toProcessedPath(uri))
}

func (w *GoogleWatcher) toProcessedPath(path string) string {
	return fmt.Sprintf("gs://%s/%s/processed/%s", w.BucketName, w.BucketCompletedPath, filepath.Base(path))
}
