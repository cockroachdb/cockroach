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

var osVersionRe = regexp.MustCompile(`\d+(\.\d+)*-`)

var isReleaseF = flag.Bool("release", false, "build in release mode instead of bleeding-edge mode")
var destBucket = flag.String("bucket", "", "override default bucket")
var doProvisionalF = flag.Bool("provisional", false, "publish provisional binaries")
var doBlessF = flag.Bool("bless", false, "bless provisional binaries")

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

			absolutePath := filepath.Join(o.PkgDir, o.Base)
			binary, err := os.Open(absolutePath)
			if err != nil {
				log.Fatalf("os.Open(%s): %s", absolutePath, err)
			}
			if !flags.isRelease {
				putNonRelease(svc, o, binary)
			} else {
				putRelease(svc, o, binary)
			}
			if err := binary.Close(); err != nil {
				log.Fatal(err)
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

// TrimDotExe trims '.exe. from `name` and returns the result (and whether any
// trimming has occurred).
func TrimDotExe(name string) (string, bool) {
	return strings.TrimSuffix(name, dotExe), strings.HasSuffix(name, dotExe)
}

func putNonRelease(svc s3I, o opts, binary io.ReadSeeker) {
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
	log.Printf("Uploading to s3://%s%s", o.BucketName, versionKey)
	if _, err := svc.PutObject(&s3.PutObjectInput{
		Bucket:             &o.BucketName,
		ContentDisposition: &disposition,
		Key:                &versionKey,
		Body:               binary,
	}); err != nil {
		log.Fatalf("s3 upload %s: %s", versionKey, err)
	}
	latestSuffix := o.Branch
	if latestSuffix == "master" {
		latestSuffix = "LATEST"
	}
	latestKey := fmt.Sprintf("%s/%s.%s", repoName, remoteName, latestSuffix)
	log.Printf("Uploading to s3://%s/%s", o.BucketName, latestKey)
	if _, err := svc.PutObject(&s3.PutObjectInput{
		Bucket:                  &o.BucketName,
		CacheControl:            &noCache,
		Key:                     &latestKey,
		WebsiteRedirectLocation: &versionKey,
	}); err != nil {
		log.Fatalf("s3 redirect to %s: %s", versionKey, err)
	}
}

func s3KeyRelease(o opts) (string, string) {
	targetSuffix, hasExe := TrimDotExe(o.Suffix)
	// TODO(tamird): remove this weirdness. Requires updating
	// "users" e.g. docs, cockroachdb/cockroach-go, maybe others.
	if strings.Contains(o.BuildType, "linux") {
		targetSuffix = strings.Replace(targetSuffix, "gnu-", "", -1)
		targetSuffix = osVersionRe.ReplaceAllLiteralString(targetSuffix, "")
	}

	archiveBase := fmt.Sprintf("cockroach-%s", o.VersionStr)
	targetArchiveBase := archiveBase + targetSuffix
	if hasExe {
		return targetArchiveBase, targetArchiveBase + ".zip"
	}
	return targetArchiveBase, targetArchiveBase + ".tgz"
}

func s3KeyArchive(o opts) (string, string) {
	archiveBase := fmt.Sprintf("cockroach-%s", o.VersionStr)
	srcArchive := fmt.Sprintf("%s.%s", archiveBase, "src.tgz")
	return archiveBase, srcArchive
}

func putRelease(svc s3I, o opts, binary *os.File) {
	// Stat the binary. Info is needed for archive headers.
	binaryInfo, err := binary.Stat()
	if err != nil {
		log.Fatal(err)
	}

	targetArchiveBase, targetArchive := s3KeyRelease(o)
	var body bytes.Buffer
	if _, hasExe := TrimDotExe(o.Suffix); hasExe {
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
		if _, err := io.Copy(zfw, binary); err != nil {
			log.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			log.Fatal(err)
		}
	} else {
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

		if _, err := io.Copy(tw, binary); err != nil {
			log.Fatal(err)
		}
		if err := tw.Close(); err != nil {
			log.Fatal(err)
		}
		if err := gzw.Close(); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("Uploading to s3://%s/%s", o.BucketName, targetArchive)
	putObjectInput := s3.PutObjectInput{
		Bucket: &o.BucketName,
		Key:    &targetArchive,
		Body:   bytes.NewReader(body.Bytes()),
	}
	if _, err := svc.PutObject(&putObjectInput); err != nil {
		log.Fatalf("s3 upload %s: %s", targetArchive, err)
	}
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
		CacheControl: &noCache,
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
		CacheControl: &noCache,
	}
	if _, err := svc.PutObject(&putObjectInput); err != nil {
		log.Fatalf("s3 upload %s: %s", keyLatest, err)
	}
}
