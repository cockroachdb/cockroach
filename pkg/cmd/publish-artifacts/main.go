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
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"go/build"
	"io"
	"log"
	"mime"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

var osVersionRe = regexp.MustCompile(`\d+(\.\d+)*-`)

var isRelease = flag.Bool("release", false, "build in release mode instead of bleeding-edge mode")
var destBucket = flag.String("bucket", "", "override default bucket")

var (
	noCache = "no-cache"
	// TODO(tamird,benesch,bdarnell): make "latest" a website-redirect
	// rather than a full key. This means that the actual artifact will no
	// longer be named "-latest".
	latestStr = "latest"
)

const dotExe = ".exe"

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
			putObjectInput.CacheControl = &noCache
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
	{
		var err error
		o.Binary, err = os.Open(o.AbsolutePath)

		if err != nil {
			log.Fatalf("os.Open(%s): %s", o.AbsolutePath, err)
		}
	}

	if !*isRelease {
		putNonRelease(svc, o)
	} else {
		putRelease(svc, o)
	}
	if err := o.Binary.Close(); err != nil {
		log.Fatal(err)
	}
}

func buildOneWorkload(svc s3putter, o opts) {
	defer func() {
		log.Printf("done building workload: %s", pretty.Sprint(o))
	}()

	if *isRelease {
		log.Fatalf("refusing to build workload in release mode")
	}

	{
		cmd := exec.Command("make", "bin/workload")
		cmd.Dir = o.PkgDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		if err := cmd.Run(); err != nil {
			log.Fatalf("%s: %s", cmd.Args, err)
		}
	}

	o.Base = "workload"
	o.AbsolutePath = filepath.Join(o.PkgDir, "bin", o.Base)
	{
		var err error
		o.Binary, err = os.Open(o.AbsolutePath)

		if err != nil {
			log.Fatalf("os.Open(%s): %s", o.AbsolutePath, err)
		}
	}
	putNonRelease(svc, o)
	if err := o.Binary.Close(); err != nil {
		log.Fatal(err)
	}
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
	Binary       *os.File
	AbsolutePath string
	PkgDir       string
}

// TrimDotExe trims '.exe. from `name` and returns the result (and whether any
// trimming has occurred).
func TrimDotExe(name string) (string, bool) {
	return strings.TrimSuffix(name, dotExe), strings.HasSuffix(name, dotExe)
}

func putNonRelease(svc s3putter, o opts) {
	const repoName = "cockroach"
	remoteName, hasExe := TrimDotExe(o.Base)
	// TODO(tamird): do we want to keep doing this? No longer
	// doing so requires updating cockroachlabs/production, and
	// possibly cockroachdb/cockroach-go.
	remoteName = osVersionRe.ReplaceAllLiteralString(remoteName, "")

	fileName := fmt.Sprintf("%s.%s", remoteName, o.VersionStr)
	if hasExe {
		fileName += ".exe"
	}
	disposition := mime.FormatMediaType("attachment", map[string]string{"filename": fileName})

	// NB: The leading slash is required to make redirects work
	// correctly since we reuse this key as the redirect location.
	versionKey := fmt.Sprintf("/%s/%s", repoName, fileName)
	if _, err := svc.PutObject(&s3.PutObjectInput{
		Bucket:             &o.BucketName,
		ContentDisposition: &disposition,
		Key:                &versionKey,
		Body:               o.Binary,
	}); err != nil {
		log.Fatalf("s3 upload %s: %s", o.AbsolutePath, err)
	}
	latestSuffix := o.Branch
	if latestSuffix == "master" {
		latestSuffix = "LATEST"
	}
	latestKey := fmt.Sprintf("%s/%s.%s", repoName, remoteName, latestSuffix)
	if _, err := svc.PutObject(&s3.PutObjectInput{
		Bucket:                  &o.BucketName,
		CacheControl:            &noCache,
		Key:                     &latestKey,
		WebsiteRedirectLocation: &versionKey,
	}); err != nil {
		log.Fatalf("s3 redirect to %s: %s", versionKey, err)
	}
}

func putRelease(svc s3putter, o opts) {
	targetSuffix, hasExe := TrimDotExe(o.Suffix)
	// TODO(tamird): remove this weirdness. Requires updating
	// "users" e.g. docs, cockroachdb/cockroach-go, maybe others.
	if strings.Contains(o.BuildType, "linux") {
		targetSuffix = strings.Replace(targetSuffix, "gnu-", "", -1)
		targetSuffix = osVersionRe.ReplaceAllLiteralString(targetSuffix, "")
	}

	// Stat the binary. Info is needed for archive headers.
	binaryInfo, err := o.Binary.Stat()
	if err != nil {
		log.Fatal(err)
	}

	for _, releaseVersionStr := range o.ReleaseVersionStrs {
		archiveBase := fmt.Sprintf("cockroach-%s", releaseVersionStr)
		targetArchiveBase := archiveBase + targetSuffix
		var targetArchive string
		var body bytes.Buffer
		if hasExe {
			targetArchive = targetArchiveBase + ".zip"
			zw := zip.NewWriter(&body)

			// Set the zip header from the file info. Overwrite name.
			zipHeader, err := zip.FileInfoHeader(binaryInfo)
			if err != nil {
				log.Fatal(err)
			}
			zipHeader.Name = filepath.Join(targetArchiveBase, "cockroach.exe")

			zfw, err := zw.CreateHeader(zipHeader)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := io.Copy(zfw, o.Binary); err != nil {
				log.Fatal(err)
			}
			if err := zw.Close(); err != nil {
				log.Fatal(err)
			}
		} else {
			targetArchive = targetArchiveBase + ".tgz"
			gzw := gzip.NewWriter(&body)
			tw := tar.NewWriter(gzw)

			// Set the tar header from the file info. Overwrite name.
			tarHeader, err := tar.FileInfoHeader(binaryInfo, "")
			if err != nil {
				log.Fatal(err)
			}
			tarHeader.Name = filepath.Join(targetArchiveBase, "cockroach")
			if err := tw.WriteHeader(tarHeader); err != nil {
				log.Fatal(err)
			}

			if _, err := io.Copy(tw, o.Binary); err != nil {
				log.Fatal(err)
			}
			if err := tw.Close(); err != nil {
				log.Fatal(err)
			}
			if err := gzw.Close(); err != nil {
				log.Fatal(err)
			}
		}
		if _, err := o.Binary.Seek(0, 0); err != nil {
			log.Fatal(err)
		}
		putObjectInput := s3.PutObjectInput{
			Bucket: &o.BucketName,
			Key:    &targetArchive,
			Body:   bytes.NewReader(body.Bytes()),
		}
		if releaseVersionStr == latestStr {
			putObjectInput.CacheControl = &noCache
		}
		if _, err := svc.PutObject(&putObjectInput); err != nil {
			log.Fatalf("s3 upload %s: %s", targetArchive, err)
		}
	}
}
