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

	"github.com/cockroachdb/cockroach/pkg/release"
	"github.com/kr/pretty"
)

const (
	awsAccessKeyIDKey      = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyKey  = "AWS_SECRET_ACCESS_KEY"
	teamcityBuildBranchKey = "TC_BUILD_BRANCH"
)

func main() {
	var s3Bucket string
	var gcsBucket string
	flag.StringVar(&s3Bucket, "bucket", "", "S3 bucket")
	flag.StringVar(&gcsBucket, "gcs-bucket", "", "GCS bucket")
	flag.Parse()

	if gcsBucket == "" {
		log.Fatal("GCS bucket is not set")
	}
	if _, ok := os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS"); !ok {
		log.Fatal("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set")
	}
	gcs, err := release.NewGCS(gcsBucket)
	if err != nil {
		log.Fatalf("Creating GCS session: %s", err)
	}
	var providers []release.ObjectPutGetter
	providers = append(providers, gcs)

	if s3Bucket != "" {
		log.Printf("Using S3 bucket: %s", s3Bucket)
		if _, ok := os.LookupEnv(awsAccessKeyIDKey); !ok {
			log.Fatalf("AWS access key ID environment variable %s is not set", awsAccessKeyIDKey)
		}
		if _, ok := os.LookupEnv(awsSecretAccessKeyKey); !ok {
			log.Fatalf("AWS secret access key environment variable %s is not set", awsSecretAccessKeyKey)
		}
		s3, err := release.NewS3("us-east-1", s3Bucket)
		if err != nil {
			log.Fatalf("Creating AWS S3 session: %s", err)
		}
		providers = append(providers, s3)
	} else {
		log.Println("Not using S3 bucket")
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

	run(providers, runFlags{
		pkgDir: pkg,
		branch: branch,
		sha:    versionStr,
	}, release.ExecFn{})
}

type runFlags struct {
	branch string
	sha    string
	pkgDir string
}

func run(providers []release.ObjectPutGetter, flags runFlags, execFn release.ExecFn) {
	for _, platform := range []release.Platform{
		release.PlatformLinux,
		release.PlatformLinuxArm,
		release.PlatformMacOS,
		release.PlatformMacOSArm,
		release.PlatformWindows,
	} {
		var o opts
		o.Platform = platform
		o.ReleaseVersions = []string{flags.sha}
		o.PkgDir = flags.pkgDir
		o.Branch = flags.branch
		o.VersionStr = flags.sha
		o.AbsolutePath = filepath.Join(flags.pkgDir, "cockroach"+release.SuffixFromPlatform(platform))
		o.CockroachSQLAbsolutePath = filepath.Join(flags.pkgDir, "cockroach-sql"+release.SuffixFromPlatform(platform))

		log.Printf("building %s", pretty.Sprint(o))
		buildOneCockroach(providers, o, execFn)
	}
	// We build workload only for Linux.
	var o opts
	o.Platform = release.PlatformLinux
	o.PkgDir = flags.pkgDir
	o.Branch = flags.branch
	o.VersionStr = flags.sha
	buildAndPublishWorkload(providers, o, execFn)
}

func buildOneCockroach(providers []release.ObjectPutGetter, o opts, execFn release.ExecFn) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	if err := release.MakeRelease(o.Platform, release.BuildOptions{ExecFn: execFn}, o.PkgDir); err != nil {
		log.Fatal(err)
	}
	for _, provider := range providers {
		release.PutNonRelease(
			provider,
			release.PutNonReleaseOptions{
				Branch: o.Branch,
				Files: append(
					[]release.NonReleaseFile{
						release.MakeCRDBBinaryNonReleaseFile(o.AbsolutePath, o.VersionStr),
						release.MakeCRDBBinaryNonReleaseFile(o.CockroachSQLAbsolutePath, o.VersionStr),
					},
					release.MakeCRDBLibraryNonReleaseFiles(o.PkgDir, o.Platform, o.VersionStr)...,
				),
			},
		)
	}
	log.Printf("done building cockroach: %s", pretty.Sprint(o))
}

func buildAndPublishWorkload(providers []release.ObjectPutGetter, o opts, execFn release.ExecFn) {
	log.Printf("building workload %s", pretty.Sprint(o))
	if err := release.MakeWorkload(release.BuildOptions{ExecFn: execFn}, o.PkgDir); err != nil {
		log.Fatal(err)
	}
	o.AbsolutePath = filepath.Join(o.PkgDir, "bin", "workload")
	for _, provider := range providers {
		release.PutNonRelease(
			provider,
			release.PutNonReleaseOptions{
				Branch: o.Branch,
				Files: []release.NonReleaseFile{
					release.MakeCRDBBinaryNonReleaseFile(o.AbsolutePath, o.VersionStr),
				},
			},
		)
	}
	log.Printf("done building workload: %s", pretty.Sprint(o))
}

type opts struct {
	VersionStr               string
	Branch                   string
	ReleaseVersions          []string
	Platform                 release.Platform
	AbsolutePath             string
	CockroachSQLAbsolutePath string
	PkgDir                   string
}
