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
	"io"
	"log"
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
	execFn := release.DefaultExecFn
	branch, ok := os.LookupEnv(teamcityBuildBranchKey)
	if !ok {
		log.Fatalf("VCS branch environment variable %s is not set", teamcityBuildBranchKey)
	}
	pkg, err := build.Import("github.com/cockroachdb/cockroach", "", build.FindOnly)
	if err != nil {
		log.Fatalf("unable to locate CRDB directory: %s", err)
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = pkg.Dir
	log.Printf("%s %s", cmd.Env, cmd.Args)
	shaOut, err := cmd.Output()
	if err != nil {
		log.Fatalf("%s: out=%q err=%s", cmd.Args, shaOut, err)
	}

	run(s3, execFn, runFlags{
		doProvisional: *doProvisionalF,
		doBless:       *doBlessF,
		isRelease:     *isReleaseF,
		branch:        branch,
		pkgDir:        pkg.Dir,
		sha:           string(bytes.TrimSpace(shaOut)),
	})
}

type runFlags struct {
	doProvisional, doBless bool
	isRelease              bool
	branch, sha            string
	pkgDir                 string
}

func run(svc s3I, execFn release.ExecFn, flags runFlags) {
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
			//
			// As of 2019-01-08, we don't really use this for anything, so save the build time.
			// {suffix: ".race", goflags: "-race"},
		} {
			var o opts
			o.PkgDir = flags.pkgDir
			o.Branch = flags.branch
			o.VersionStr = versionStr
			o.BucketName = bucketName
			o.BuildType = target.BuildType
			o.GoFlags = extraArgs.goflags
			o.Suffix = extraArgs.suffix + target.Suffix
			o.Tags = extraArgs.tags
			o.Base = "cockroach" + o.Suffix

			// TODO(tamird): build deadlock,race binaries for all targets?
			if i > 0 && (flags.isRelease || !strings.HasSuffix(o.BuildType, "linux-gnu")) {
				log.Printf("skipping auxiliary build: %s", pretty.Sprint(o))
				continue
			}

			cockroachBuildOpts = append(cockroachBuildOpts, o)
		}
	}
	archiveBuildOpts := opts{
		PkgDir:     flags.pkgDir,
		BucketName: bucketName,
		VersionStr: versionStr,
	}

	if flags.doProvisional {
		for _, o := range cockroachBuildOpts {
			buildCockroach(execFn, flags, o)

			if !flags.isRelease {
				putNonRelease(svc, o)
			} else {
				putRelease(svc, o)
			}
		}
		if flags.isRelease {
			buildAndPutArchive(svc, execFn, archiveBuildOpts)
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
			markLatestArchive(svc, archiveBuildOpts)
		}
	}
}

func buildAndPutArchive(svc s3I, execFn release.ExecFn, o opts) {
	log.Printf("building archive %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building archive: %s", pretty.Sprint(o))
	}()

	archiveBase, srcArchive := s3KeyArchive(o)
	cmd := exec.Command(
		"make",
		"archive",
		fmt.Sprintf("ARCHIVE_BASE=%s", archiveBase),
		fmt.Sprintf("ARCHIVE=%s", srcArchive),
		fmt.Sprintf("BUILDINFO_TAG=%s", o.VersionStr),
	)
	cmd.Dir = o.PkgDir
	cmd.Stderr = os.Stderr
	log.Printf("%s %s", cmd.Env, cmd.Args)
	if out, err := execFn(cmd); err != nil {
		log.Fatalf("%s %s: %s\n\n%s", cmd.Env, cmd.Args, err, out)
	}

	absoluteSrcArchivePath := filepath.Join(o.PkgDir, srcArchive)
	f, err := os.Open(absoluteSrcArchivePath)
	if err != nil {
		log.Fatalf("os.Open(%s): %s", absoluteSrcArchivePath, err)
	}
	log.Printf("Uploading to s3://%s/%s", o.BucketName, srcArchive)
	putObjectInput := s3.PutObjectInput{
		Bucket: &o.BucketName,
		Key:    &srcArchive,
		Body:   f,
	}
	if _, err := svc.PutObject(&putObjectInput); err != nil {
		log.Fatalf("s3 upload %s: %s", absoluteSrcArchivePath, err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
}

func buildCockroach(execFn release.ExecFn, flags runFlags, o opts) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building cockroach: %s", pretty.Sprint(o))
	}()

	opts := []release.MakeReleaseOption{
		release.WithMakeReleaseOptionExecFn(execFn),
		release.WithMakeReleaseOptionBuildArg(fmt.Sprintf("%s=%s", "GOFLAGS", o.GoFlags)),
		release.WithMakeReleaseOptionBuildArg(fmt.Sprintf("%s=%s", "TAGS", o.Tags)),
		release.WithMakeReleaseOptionBuildArg(fmt.Sprintf("%s=%s", "BUILDCHANNEL", "official-binary")),
	}
	if flags.isRelease {
		opts = append(opts, release.WithMakeReleaseOptionBuildArg(fmt.Sprintf("%s=%s", "BUILDINFO_TAG", o.VersionStr)))
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
}

type opts struct {
	VersionStr string
	Branch     string

	BuildType string
	GoFlags   string
	Suffix    string
	Tags      string

	Base       string
	BucketName string
	PkgDir     string
}

func putNonRelease(svc s3I, o opts) {
	release.PutNonRelease(
		svc,
		release.PutNonReleaseOptions{
			Branch:     o.Branch,
			BucketName: o.BucketName,
			Files: append(
				[]release.NonReleaseFile{release.MakeCRDBBinaryNonReleaseFile(o.Base, filepath.Join(o.PkgDir, o.Base), o.VersionStr)},
				release.MakeCRDBLibraryNonReleaseFiles(o.PkgDir, o.BuildType, o.VersionStr, o.Suffix)...,
			),
		},
	)
}

func s3KeyRelease(o opts) (string, string) {
	return release.S3KeyRelease(o.Suffix, o.BuildType, o.VersionStr)
}

func s3KeyArchive(o opts) (string, string) {
	archiveBase := fmt.Sprintf("cockroach-%s", o.VersionStr)
	srcArchive := fmt.Sprintf("%s.%s", archiveBase, "src.tgz")
	return archiveBase, srcArchive
}

func putRelease(svc s3I, o opts) {
	release.PutRelease(svc, release.PutReleaseOptions{
		BucketName: o.BucketName,
		NoCache:    false,
		Suffix:     o.Suffix,
		BuildType:  o.BuildType,
		VersionStr: o.VersionStr,
		Files: append(
			[]release.ArchiveFile{release.MakeCRDBBinaryArchiveFile(o.Base, filepath.Join(o.PkgDir, o.Base))},
			release.MakeCRDBLibraryArchiveFiles(o.PkgDir, o.BuildType)...,
		),
	})
}

func markLatestRelease(svc s3I, o opts) {
	_, keyRelease := s3KeyRelease(o)
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

func markLatestArchive(svc s3I, o opts) {
	_, keyRelease := s3KeyArchive(o)

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
	_, keyLatest := s3KeyArchive(oLatest)
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
