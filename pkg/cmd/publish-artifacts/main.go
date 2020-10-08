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
	"fmt"
	"go/build"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/pkg/release"
	"github.com/cockroachdb/cockroach/pkg/util/version"
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

var isRelease = flag.Bool("release", false, "build in release mode instead of bleeding-edge mode")
var destBucket = flag.String("bucket", "", "override default bucket")

var (
	// TODO(tamird,benesch,bdarnell): make "latest" a website-redirect
	// rather than a full key. This means that the actual artifact will no
	// longer be named "-latest".
	latestStr = "latest"
)

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
	pkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}

	var versionStr string
	var isStableRelease bool
	if *isRelease {
		ver, err := version.Parse(branch)
		if err != nil {
			log.Fatalf("refusing to build release with invalid version name '%s' (err: %s)", branch, err)
		}

		// Prerelease returns anything after the `-` and before metadata. eg: `beta` for `1.0.1-beta+metadata`
		if ver.PreRelease() == "" {
			isStableRelease = true
		}
		versionStr = branch
	} else {
		cmd := exec.Command("git", "rev-parse", "HEAD")
		cmd.Dir = pkg.Dir
		log.Printf("%s %s", cmd.Env, cmd.Args)
		out, err := cmd.Output()
		if err != nil {
			log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
		}
		versionStr = string(bytes.TrimSpace(out))
	}

	svc, err := testableS3()
	if err != nil {
		log.Fatalf("Creating AWS S3 session: %s", err)
	}

	var bucketName string
	if len(*destBucket) > 0 {
		bucketName = *destBucket
	} else if *isRelease {
		bucketName = "binaries.cockroachdb.com"
	} else {
		bucketName = "cockroach"
	}
	log.Printf("Using S3 bucket: %s", bucketName)

	releaseVersionStrs := []string{versionStr}
	// Only build `latest` tarballs for stable releases.
	if isStableRelease {
		releaseVersionStrs = append(releaseVersionStrs, latestStr)
	}

	if *isRelease {
		buildArchive(svc, opts{
			PkgDir:             pkg.Dir,
			BucketName:         bucketName,
			ReleaseVersionStrs: releaseVersionStrs,
		})
	}

	for _, target := range release.SupportedTargets {
		for i, extraArgs := range []struct {
			goflags string
			suffix  string
			tags    string
		}{
			{},
			// TODO(tamird): re-enable deadlock builds. This really wants its
			// own install suffix; it currently pollutes the normal release
			// build cache.
			//
			// {suffix: ".deadlock", tags: "deadlock"},
			{suffix: ".race", goflags: "-race"},
		} {
			var o opts
			o.ReleaseVersionStrs = releaseVersionStrs
			o.PkgDir = pkg.Dir
			o.Branch = branch
			o.VersionStr = versionStr
			o.BucketName = bucketName
			o.Branch = branch
			o.BuildType = target.BuildType
			o.GoFlags = extraArgs.goflags
			o.Suffix = extraArgs.suffix + target.Suffix
			o.Tags = extraArgs.tags

			log.Printf("building %s", pretty.Sprint(o))

			// TODO(tamird): build deadlock,race binaries for all targets?
			if i > 0 && (*isRelease || !strings.HasSuffix(o.BuildType, "linux-gnu")) {
				log.Printf("skipping auxiliary build")
				continue
			}

			buildOneCockroach(svc, o)
		}
	}

	if !*isRelease {
		buildOneWorkload(svc, opts{
			PkgDir:     pkg.Dir,
			BucketName: bucketName,
			Branch:     branch,
			VersionStr: versionStr,
		})
	}
}

func buildArchive(svc s3putter, o opts) {
	for _, releaseVersionStr := range o.ReleaseVersionStrs {
		archiveBase := fmt.Sprintf("cockroach-%s", releaseVersionStr)
		srcArchive := fmt.Sprintf("%s.%s", archiveBase, "src.tgz")
		cmd := exec.Command(
			"make",
			"archive",
			fmt.Sprintf("ARCHIVE_BASE=%s", archiveBase),
			fmt.Sprintf("ARCHIVE=%s", srcArchive),
		)
		cmd.Dir = o.PkgDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		if err := cmd.Run(); err != nil {
			log.Fatalf("%s: %s", cmd.Args, err)
		}

		absoluteSrcArchivePath := filepath.Join(o.PkgDir, srcArchive)
		f, err := os.Open(absoluteSrcArchivePath)
		if err != nil {
			log.Fatalf("os.Open(%s): %s", absoluteSrcArchivePath, err)
		}
		putObjectInput := s3.PutObjectInput{
			Bucket: &o.BucketName,
			Key:    &srcArchive,
			Body:   f,
		}
		if releaseVersionStr == latestStr {
			putObjectInput.CacheControl = &release.NoCache
		}
		if _, err := svc.PutObject(&putObjectInput); err != nil {
			log.Fatalf("s3 upload %s: %s", absoluteSrcArchivePath, err)
		}
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func buildOneCockroach(svc s3putter, o opts) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building cockroach: %s", pretty.Sprint(o))
	}()

	opts := []release.MakeReleaseOption{
		release.WithMakeReleaseOptionBuildArg(fmt.Sprintf("%s=%s", "GOFLAGS", o.GoFlags)),
		release.WithMakeReleaseOptionBuildArg(fmt.Sprintf("%s=%s", "TAGS", o.Tags)),
		release.WithMakeReleaseOptionBuildArg(fmt.Sprintf("%s=%s", "BUILDCHANNEL", "official-binary")),
	}
	if *isRelease {
		opts = append(opts, release.WithMakeReleaseOptionBuildArg(fmt.Sprintf("%s=%s", "BUILD_TAGGED_RELEASE", "true")))
	}

	if err := release.MakeRelease(
		release.SupportedTarget{
			BuildType: o.BuildType,
			Suffix:    o.Suffix,
		},
		o.PkgDir,
		opts...,
	); err != nil {
		log.Fatal(err)
	}

	o.Base = "cockroach" + o.Suffix
	o.AbsolutePath = filepath.Join(o.PkgDir, o.Base)

	if !*isRelease {
		putNonRelease(svc, o, release.MakeCRDBLibraryNonReleaseFiles(o.PkgDir, o.BuildType, o.VersionStr, o.Suffix)...)
	} else {
		putRelease(svc, o)
	}
}

func buildOneWorkload(svc s3putter, o opts) {
	defer func() {
		log.Printf("done building workload: %s", pretty.Sprint(o))
	}()

	if *isRelease {
		log.Fatalf("refusing to build workload in release mode")
	}

	if err := release.MakeWorkload(o.PkgDir); err != nil {
		log.Fatal(err)
	}

	o.Base = "workload"
	o.AbsolutePath = filepath.Join(o.PkgDir, "bin", o.Base)
	putNonRelease(svc, o)
}

type opts struct {
	VersionStr         string
	Branch             string
	ReleaseVersionStrs []string

	BuildType string
	GoFlags   string
	Suffix    string
	Tags      string

	Base         string
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
				[]release.NonReleaseFile{release.MakeCRDBBinaryNonReleaseFile(o.Base, o.AbsolutePath, o.VersionStr)},
				additionalNonReleaseFiles...,
			),
		},
	)
}

func putRelease(svc s3putter, o opts) {
	for _, releaseVersionStr := range o.ReleaseVersionStrs {
		release.PutRelease(svc, release.PutReleaseOptions{
			BucketName: o.BucketName,
			NoCache:    releaseVersionStr == latestStr,
			Suffix:     o.Suffix,
			BuildType:  o.BuildType,
			VersionStr: releaseVersionStr,
			Files: append(
				[]release.ArchiveFile{release.MakeCRDBBinaryArchiveFile(o.Base, o.AbsolutePath)},
				release.MakeCRDBLibraryArchiveFiles(o.PkgDir, o.BuildType)...,
			),
		})
	}
}
