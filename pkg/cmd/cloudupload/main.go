// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/release"
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
	case "gs":
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

func gcsUpload(bucket string, dstObj release.PutObjectInput) error {
	if _, ok := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS"); !ok {
		return fmt.Errorf("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set")
	}
	gcs, err := release.NewGCS(bucket)
	if err != nil {
		return fmt.Errorf("creating GCS session: %w", err)
	}
	// Make sure the object doesn't exist. Potentially can race.
	obj := release.GetObjectInput{Key: dstObj.Key}
	if _, err := gcs.GetObject(&obj); err == nil {
		return fmt.Errorf("cannot overwrite %s in bucket %s", *dstObj.Key, bucket)
	}
	return gcs.PutObject(&dstObj)
}
