// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	teamcityBuildBranchKey = "TC_BUILD_BRANCH"
)

func main() {
	var gcsBucket string
	var platforms release.Platforms
	flag.StringVar(&gcsBucket, "gcs-bucket", "", "GCS bucket")
	flag.Var(&platforms, "platform", "platforms to build")
	flag.Parse()
	if len(platforms) == 0 {
		platforms = release.DefaultPlatforms()
	}

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
	providers := []release.ObjectPutGetter{gcs}

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

	run(providers, platforms, runFlags{
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

func run(
	providers []release.ObjectPutGetter,
	platforms release.Platforms,
	flags runFlags,
	execFn release.ExecFn,
) {
	for _, platform := range platforms {
		var o opts
		o.Platform = platform
		o.ReleaseVersions = []string{flags.sha}
		o.PkgDir = flags.pkgDir
		o.Branch = flags.branch
		o.VersionStr = flags.sha
		o.AbsolutePath = filepath.Join(flags.pkgDir, "cockroach"+release.SuffixFromPlatform(platform))
		o.CockroachSQLAbsolutePath = filepath.Join(flags.pkgDir, "cockroach-sql"+release.SuffixFromPlatform(platform))
		o.Channel = release.ChannelFromPlatform(platform)

		log.Printf("building %s", pretty.Sprint(o))
		buildOneCockroach(providers, o, execFn)

		// We build workload only for Linux.
		if platform == release.PlatformLinux || platform == release.PlatformLinuxArm {
			var o opts
			o.Platform = platform
			o.PkgDir = flags.pkgDir
			o.Branch = flags.branch
			o.VersionStr = flags.sha
			buildAndPublishWorkload(providers, o, execFn)
		}
	}
}

func buildOneCockroach(providers []release.ObjectPutGetter, o opts, execFn release.ExecFn) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	buildOpts := release.BuildOptions{
		ExecFn:  execFn,
		Channel: o.Channel,
	}
	if err := release.MakeRelease(o.Platform, buildOpts, o.PkgDir); err != nil {
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
	if err := release.MakeWorkload(o.Platform, release.BuildOptions{ExecFn: execFn}, o.PkgDir); err != nil {
		log.Fatal(err)
	}
	o.AbsolutePath = filepath.Join(o.PkgDir, "bin", "workload")
	if o.Platform == release.PlatformLinuxArm {
		o.AbsolutePath += release.SuffixFromPlatform(o.Platform)
	}
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
	Channel                  string
}
