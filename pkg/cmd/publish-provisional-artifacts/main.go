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
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

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

var provisionalReleasePrefixRE = regexp.MustCompile(`^provisional_[0-9]{12}_`)

type s3I interface {
	GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

func makeS3() (s3I, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

var isReleaseF = flag.Bool("release", false, "build in release mode instead of bleeding-edge mode")
var destBucket = flag.String("bucket", "", "override default bucket")
var doProvisionalF = flag.Bool("provisional", false, "publish provisional binaries")
var doBlessF = flag.Bool("bless", false, "bless provisional binaries")

var (
	// TODO(tamird,benesch,bdarnell): make "latest" a website-redirect
	// rather than a full key. This means that the actual artifact will no
	// longer be named "-latest".
	latestStr = "latest"
)

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if _, ok := os.LookupEnv(awsAccessKeyIDKey); !ok {
		log.Fatalf("AWS access key ID environment variable %s is not set", awsAccessKeyIDKey)
	}
	if _, ok := os.LookupEnv(awsSecretAccessKeyKey); !ok {
		log.Fatalf("AWS secret access key environment variable %s is not set", awsSecretAccessKeyKey)
	}
	s3, err := makeS3()
	if err != nil {
		log.Fatalf("Creating AWS S3 session: %s", err)
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
	shaOut, err := cmd.Output()
	if err != nil {
		log.Fatalf("%s: out=%q err=%s", cmd.Args, shaOut, err)
	}

	run(s3, runFlags{
		doProvisional: *doProvisionalF,
		doBless:       *doBlessF,
		isRelease:     *isReleaseF,
		branch:        branch,
		pkgDir:        pkg,
		sha:           string(bytes.TrimSpace(shaOut)),
	}, release.ExecFn{})
}

type runFlags struct {
	doProvisional, doBless bool
	isRelease              bool
	branch, sha            string
	pkgDir                 string
}

func run(svc s3I, flags runFlags, execFn release.ExecFn) {
	// TODO(dan): non-release builds currently aren't broken into the two
	// phases. Instead, the provisional phase does them both.
	if !flags.isRelease {
		flags.doProvisional = true
		flags.doBless = false
	}

	var versionStr string
	var updateLatest bool
	if flags.isRelease {
		// If the tag starts with "provisional_", then we're building a binary
		// that we hope will be some final release and the tag will be of the
		// form `provisional_<yyyymmddhhss>_<semver>`. If all goes well with the
		// long running tests, these bits will be released exactly as-is, so the
		// version is set to <semver> by stripping the prefix.
		versionStr = provisionalReleasePrefixRE.ReplaceAllLiteralString(flags.branch, "")

		ver, err := version.Parse(versionStr)
		if err != nil {
			log.Fatalf("refusing to build release with invalid version name '%s' (err: %s)",
				versionStr, err)
		}

		// Prerelease returns anything after the `-` and before metadata. eg:
		// `beta` for `1.0.1-beta+metadata`
		if ver.PreRelease() == "" {
			// TODO(dan): This is what it did before, but isn't this wrong? It
			// seems like it would mark a patch release of the previous minor
			// version as latest. Instead, move to something like "latest-2.0",
			// "latest-2.1".
			updateLatest = true
		}
	} else {
		versionStr = flags.sha
		updateLatest = true
	}

	var bucketName string
	if len(*destBucket) > 0 {
		bucketName = *destBucket
	} else if flags.isRelease {
		bucketName = "binaries.cockroachdb.com"
	} else {
		bucketName = "cockroach"
	}
	log.Printf("Using S3 bucket: %s", bucketName)

	var cockroachBuildOpts []opts
	for _, platform := range []release.Platform{release.PlatformLinux, release.PlatformMacOS, release.PlatformWindows} {
		var o opts
		o.Platform = platform
		o.PkgDir = flags.pkgDir
		o.Branch = flags.branch
		o.VersionStr = versionStr
		o.BucketName = bucketName
		o.AbsolutePath = filepath.Join(flags.pkgDir, "cockroach"+release.SuffixFromPlatform(platform))
		o.CockroachSQLAbsolutePath = filepath.Join(flags.pkgDir, "cockroach-sql"+release.SuffixFromPlatform(platform))
		cockroachBuildOpts = append(cockroachBuildOpts, o)
	}

	if flags.doProvisional {
		for _, o := range cockroachBuildOpts {
			buildCockroach(flags, o, execFn)

			if !flags.isRelease {
				putNonRelease(svc, o)
			} else {
				putRelease(svc, o)
			}
		}
	}
	if flags.doBless {
		if !flags.isRelease {
			log.Fatal("cannot bless non-release versions")
		}
		if updateLatest {
			for _, o := range cockroachBuildOpts {
				markLatestRelease(svc, o)
			}
		}
	}
}

func buildCockroach(flags runFlags, o opts, execFn release.ExecFn) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building cockroach: %s", pretty.Sprint(o))
	}()

	var buildOpts release.BuildOptions
	buildOpts.ExecFn = execFn
	if flags.isRelease {
		buildOpts.Release = true
		buildOpts.BuildTag = o.VersionStr
	}

	if err := release.MakeRelease(o.Platform, buildOpts, o.PkgDir); err != nil {
		log.Fatal(err)
	}
}

type opts struct {
	VersionStr string
	Branch     string

	Platform release.Platform

	AbsolutePath             string
	CockroachSQLAbsolutePath string
	BucketName               string
	PkgDir                   string
}

func putNonRelease(svc s3I, o opts) {
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
				release.MakeCRDBLibraryNonReleaseFiles(o.PkgDir, o.Platform, o.VersionStr)...,
			),
		},
	)
}

func s3KeyRelease(o opts) (string, string) {
	return release.S3KeyRelease(o.Platform, o.VersionStr, "cockroach")
}

func putRelease(svc s3I, o opts) {
	release.PutRelease(svc, release.PutReleaseOptions{
		BucketName:    o.BucketName,
		NoCache:       false,
		Platform:      o.Platform,
		VersionStr:    o.VersionStr,
		ArchivePrefix: "cockroach",
		Files: append(
			[]release.ArchiveFile{release.MakeCRDBBinaryArchiveFile(o.AbsolutePath, "cockroach")},
			release.MakeCRDBLibraryArchiveFiles(o.PkgDir, o.Platform)...,
		),
	})
	release.PutRelease(svc, release.PutReleaseOptions{
		BucketName:    o.BucketName,
		NoCache:       false,
		Platform:      o.Platform,
		VersionStr:    o.VersionStr,
		ArchivePrefix: "cockroach-sql",
		Files:         []release.ArchiveFile{release.MakeCRDBBinaryArchiveFile(o.AbsolutePath, "cockroach-sql")},
	})
}

func markLatestRelease(svc s3I, o opts) {
	markLatestReleaseWithSuffix(svc, o, "")
	markLatestReleaseWithSuffix(svc, o, release.ChecksumSuffix)
}

func markLatestReleaseWithSuffix(svc s3I, o opts, suffix string) {
	_, keyRelease := s3KeyRelease(o)
	keyRelease += suffix
	log.Printf("Downloading from %s/%s", o.BucketName, keyRelease)
	binary, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: &o.BucketName,
		Key:    &keyRelease,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer binary.Body.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, binary.Body); err != nil {
		log.Fatalf("downloading %s/%s: %s", o.BucketName, keyRelease, err)
	}

	oLatest := o
	oLatest.VersionStr = latestStr
	_, keyLatest := s3KeyRelease(oLatest)
	keyLatest += suffix
	log.Printf("Uploading to s3://%s/%s", o.BucketName, keyLatest)
	putObjectInput := s3.PutObjectInput{
		Bucket:       &o.BucketName,
		Key:          &keyLatest,
		Body:         bytes.NewReader(buf.Bytes()),
		CacheControl: &release.NoCache,
	}
	if _, err := svc.PutObject(&putObjectInput); err != nil {
		log.Fatalf("s3 upload %s: %s", keyLatest, err)
	}
}
