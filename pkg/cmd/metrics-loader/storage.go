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
	"fmt"
	"io"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/iterator"
)

type Storage interface {
	Read(path string) (io.ReadCloser, error)
	Move(src, dst string) error
	Delete(path string) error
	ListFiles(path string) ([]string, error)
	io.Closer
}

type GoogleStorage interface {
	Storage
	RegisterFinalizeNotification(bucketName, projectID, topicID string) error
}

type googleStorage struct {
	client *storage.Client
	ctx    context.Context
}

func NewGoogleStorage() (GoogleStorage, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &googleStorage{
		client: client,
		ctx:    ctx,
	}, nil
}

func (s *googleStorage) Close() error {
	return s.client.Close()
}

func (s *googleStorage) Read(path string) (io.ReadCloser, error) {
	bucketName, objectName, err := parseURI(path)
	if err != nil {
		return nil, err
	}

	bucket := s.client.Bucket(bucketName)
	obj := bucket.Object(objectName)
	return obj.NewReader(s.ctx)
}

func (s *googleStorage) Move(src, dst string) error {
	srcBucketName, srcObjectName, err := parseURI(src)
	if err != nil {
		return err
	}
	dstBucketName, dstObjectName, err := parseURI(dst)
	if err != nil {
		return err
	}

	srcBucket := s.client.Bucket(srcBucketName)
	srcObject := srcBucket.Object(srcObjectName)
	dstBucket := s.client.Bucket(dstBucketName)
	dstObject := dstBucket.Object(dstObjectName)

	if _, err := dstObject.CopierFrom(srcObject).Run(s.ctx); err != nil {
		return err
	}
	return srcObject.Delete(s.ctx)
}

func (s *googleStorage) Delete(path string) error {
	bucketName, objectName, err := parseURI(path)
	if err != nil {
		return err
	}

	bucket := s.client.Bucket(bucketName)
	object := bucket.Object(objectName)
	return object.Delete(s.ctx)
}

func (s *googleStorage) ListFiles(path string) ([]string, error) {
	bucketName, objectName, err := parseURI(path)
	if err != nil {
		return nil, err
	}

	delim := "/"
	if !strings.HasSuffix(objectName, delim) {
		objectName = objectName + delim
	}
	bucket := s.client.Bucket(bucketName)
	query := &storage.Query{
		Delimiter: delim,
		Prefix:    objectName,
	}

	var paths []string
	it := bucket.Objects(s.ctx, query)
	for {
		obj, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		paths = append(paths, "gs://"+bucketName+delim+obj.Name)
	}

	return paths, nil
}

func (s *googleStorage) RegisterFinalizeNotification(bucketName, projectID, topicID string) error {
	bucket := s.client.Bucket(bucketName)
	notifications, err := bucket.Notifications(s.ctx)
	if err != nil {
		return err
	}

	// Check if the notification already exists.
	for _, v := range notifications {
		if v.TopicID == topicID && v.TopicProjectID == projectID {
			return nil
		}
	}

	_, err = bucket.AddNotification(s.ctx, &storage.Notification{
		TopicProjectID: projectID,
		TopicID:        topicID,
		EventTypes:     []string{"OBJECT_FINALIZE"},
		PayloadFormat:  storage.JSONPayload,
	})
	return err
}

// parseURI parses a GCS URI into a bucket name and object name.
func parseURI(uri string) (string, string, error) {
	re := regexp.MustCompile(`gs://(.*?)/(.*)`)
	matches := re.FindStringSubmatch(uri)
	if len(matches) != 3 {
		return "", "", fmt.Errorf("invalid GCS URI: %s", uri)
	}
	return matches[1], matches[2], nil
}
