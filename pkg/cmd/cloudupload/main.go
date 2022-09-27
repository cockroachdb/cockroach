// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/release"
)

const (
	awsAccessKeyIDKey     = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyKey = "AWS_SECRET_ACCESS_KEY"
	providerS3            = "s3"
	providerGCS           = "gcs"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("usage: cloudupload <src> <dst>")
	}
	src := os.Args[1]
	dst := os.Args[2]
	handler, err := os.Open(src)
	if err != nil {
		log.Fatalf("Cannot open %s: %s", src, err)
	}
	parsedDst, err := parseURL(dst)
	if err != nil {
		log.Fatalf("Cannot parse %s: %s", dst, err)
	}
	dstObj := release.PutObjectInput{
		Key:  &parsedDst.key,
		Body: handler,
	}
	switch parsedDst.provider {
	case providerS3:
		if err := s3Upload(parsedDst.bucket, dstObj); err != nil {
			log.Fatalf("failed to upload %s to %s: %s", src, dst, err)
		}
	case providerGCS:
		if err := gcsUpload(parsedDst.bucket, dstObj); err != nil {
			log.Fatalf("failed to upload %s to %s: %s", src, dst, err)
		}
	default:
		log.Fatalf("Unsupported destination format: %s", dst)
	}
}

type target struct {
	provider string
	bucket   string
	key      string
}

func parseURL(dst string) (target, error) {
	parsed, err := url.Parse(dst)
	if err != nil {
		return target{}, fmt.Errorf("cannot parse %s: %w", dst, err)
	}
	return target{
		provider: parsed.Scheme,
		bucket:   parsed.Host,
		key:      strings.TrimPrefix(parsed.Path, "/"),
	}, nil
}

func s3Upload(bucket string, dstObj release.PutObjectInput) error {
	if _, ok := os.LookupEnv(awsAccessKeyIDKey); !ok {
		return fmt.Errorf("AWS access key ID environment variable %s is not set", awsAccessKeyIDKey)
	}
	if _, ok := os.LookupEnv(awsSecretAccessKeyKey); !ok {
		return fmt.Errorf("AWS secret access key environment variable %s is not set", awsSecretAccessKeyKey)
	}
	s3, err := release.NewS3("us-east-1", bucket)
	if err != nil {
		return fmt.Errorf("creating AWS S3 session: %s", err)
	}
	return s3.PutObject(&dstObj)
}

func gcsUpload(bucket string, dstObj release.PutObjectInput) error {
	if _, ok := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS"); !ok {
		return fmt.Errorf("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set")
	}
	gcs, err := release.NewGCS(bucket)
	if err != nil {
		return fmt.Errorf("creating GCS session: %s", err)
	}
	return gcs.PutObject(&dstObj)
}
