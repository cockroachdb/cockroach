// Copyright 2017 The Cockroach Authors.
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
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/pkg/release"
	"github.com/kr/pretty"
)

const (
	awsAccessKeyIDKey      = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyKey  = "AWS_SECRET_ACCESS_KEY"
	teamcityBuildBranchKey = "TC_BUILD_BRANCH"
)

type s3putter interface {
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

// Overridden in testing.
var testableS3 = func() (s3putter, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

var destBucket = flag.String("bucket", "", "override default bucket")

func main() {
	flag.Parse()

	if _, ok := os.LookupEnv(awsAccessKeyIDKey); !ok {
		log.Fatalf("AWS access key ID environment variable %s is not set", awsAccessKeyIDKey)
	}
	if _, ok := os.LookupEnv(awsSecretAccessKeyKey); !ok {
		log.Fatalf("AWS secret access key environment variable %s is not set", awsSecretAccessKeyKey)
	}

	branch, ok := os.LookupEnv(teamcityBuildBranchKey)
	if !ok {
		log.Fatalf("VCS branch environment variable %s is not set", teamcityBuildBranchKey)
	}
	pkg, err := os.Getwd()
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}
	// Make sure the WORKSPACE file is in the current working directory.
	_, err = os.Stat(filepath.Join(pkg, "WORKSPACE"))
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}

	var versionStr string

	svc, err := testableS3()
	if err != nil {
		log.Fatalf("Creating AWS S3 session: %s", err)
	}

	var bucketName string
	if len(*destBucket) > 0 {
		bucketName = *destBucket
	} else {
		bucketName = "cockroach"
	}
	log.Printf("Using S3 bucket: %s", bucketName)

	releaseVersionStrs := []string{versionStr}

	for _, platform := range []release.Platform{release.PlatformLinux, release.PlatformMacOS, release.PlatformWindows} {
		var o opts
		o.Platform = platform
		o.ReleaseVersionStrs = releaseVersionStrs
		o.PkgDir = pkg
		o.Branch = branch
		o.VersionStr = versionStr
		o.BucketName = bucketName
		o.Branch = branch

		log.Printf("building %s", pretty.Sprint(o))

		buildOneCockroach(svc, o)
	}
}

func buildOneCockroach(svc s3putter, o opts) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building cockroach: %s", pretty.Sprint(o))
	}()

	if err := release.MakeRelease(o.Platform, release.BuildOptions{}, o.PkgDir); err != nil {
		log.Fatal(err)
	}

	putNonRelease(svc, o, release.MakeCRDBLibraryNonReleaseFiles(o.PkgDir, o.Platform, o.VersionStr)...)
}

type opts struct {
	VersionStr         string
	Branch             string
	ReleaseVersionStrs []string

	Platform release.Platform

	BucketName   string
	AbsolutePath string
	PkgDir       string
}

func putNonRelease(svc s3putter, o opts, additionalNonReleaseFiles ...release.NonReleaseFile) {
	release.PutNonRelease(
		svc,
		release.PutNonReleaseOptions{
			Branch:     o.Branch,
			BucketName: o.BucketName,
			Files: append(
				[]release.NonReleaseFile{release.MakeCRDBBinaryNonReleaseFile(o.AbsolutePath, o.VersionStr)},
				additionalNonReleaseFiles...,
			),
		},
	)
}
