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
	"bytes"
	"flag"
	"log"
	"os"
	"os/exec"
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

	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = pkg
	log.Printf("%s %s", cmd.Env, cmd.Args)
	out, err := cmd.Output()
	if err != nil {
		log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
	}
	versionStr := string(bytes.TrimSpace(out))

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

	run(svc, runFlags{
		pkgDir:     pkg,
		branch:     branch,
		sha:        versionStr,
		bucketName: bucketName,
	}, release.ExecFn{})
}

type runFlags struct {
	branch, sha string
	pkgDir      string
	bucketName  string
}

func run(svc s3putter, flags runFlags, execFn release.ExecFn) {
	for _, platform := range []release.Platform{release.PlatformLinux, release.PlatformLinuxArm, release.PlatformMacOS, release.PlatformWindows} {
		var o opts
		o.Platform = platform
		o.ReleaseVersionStrs = []string{flags.sha}
		o.PkgDir = flags.pkgDir
		o.Branch = flags.branch
		o.VersionStr = flags.sha
		o.BucketName = flags.bucketName
		o.AbsolutePath = filepath.Join(flags.pkgDir, "cockroach"+release.SuffixFromPlatform(platform))
		o.CockroachSQLAbsolutePath = filepath.Join(flags.pkgDir, "cockroach-sql"+release.SuffixFromPlatform(platform))

		log.Printf("building %s", pretty.Sprint(o))

		buildOneCockroach(svc, o, execFn)
	}
	// We build workload only for Linux.
	var o opts
	o.Platform = release.PlatformLinux
	o.PkgDir = flags.pkgDir
	o.BucketName = flags.bucketName
	o.Branch = flags.branch
	o.VersionStr = flags.sha
	buildOneWorkload(svc, o, execFn)
}

func buildOneCockroach(svc s3putter, o opts, execFn release.ExecFn) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building cockroach: %s", pretty.Sprint(o))
	}()

	if err := release.MakeRelease(o.Platform, release.BuildOptions{ExecFn: execFn}, o.PkgDir); err != nil {
		log.Fatal(err)
	}

	putNonRelease(svc, o, release.MakeCRDBLibraryNonReleaseFiles(o.PkgDir, o.Platform, o.VersionStr)...)
}

func buildOneWorkload(svc s3putter, o opts, execFn release.ExecFn) {
	defer func() {
		log.Printf("done building workload: %s", pretty.Sprint(o))
	}()

	if err := release.MakeWorkload(release.BuildOptions{ExecFn: execFn}, o.PkgDir); err != nil {
		log.Fatal(err)
	}
	o.AbsolutePath = filepath.Join(o.PkgDir, "bin", "workload")
	release.PutNonRelease(
		svc,
		release.PutNonReleaseOptions{
			Branch:     o.Branch,
			BucketName: o.BucketName,
			Files: []release.NonReleaseFile{
				release.MakeCRDBBinaryNonReleaseFile(o.AbsolutePath, o.VersionStr),
			},
		},
	)
}

type opts struct {
	VersionStr         string
	Branch             string
	ReleaseVersionStrs []string

	Platform release.Platform

	BucketName               string
	AbsolutePath             string
	CockroachSQLAbsolutePath string
	PkgDir                   string
}

func putNonRelease(svc s3putter, o opts, additionalNonReleaseFiles ...release.NonReleaseFile) {
	release.PutNonRelease(
		svc,
		release.PutNonReleaseOptions{
			Branch:     o.Branch,
			BucketName: o.BucketName,
			Files: append(
				[]release.NonReleaseFile{
					release.MakeCRDBBinaryNonReleaseFile(o.AbsolutePath, o.VersionStr),
					release.MakeCRDBBinaryNonReleaseFile(o.CockroachSQLAbsolutePath, o.VersionStr),
				},
				additionalNonReleaseFiles...,
			),
		},
	)
}
