// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"go/build"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/kr/pretty"
)

const (
	awsAccessKeyIDKey      = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyKey  = "AWS_SECRET_ACCESS_KEY"
	teamcityBuildBranchKey = "TC_BUILD_BRANCH"
)

var provisionalReleasePrefixRE = regexp.MustCompile(`^provisional_[0-9]{12}_`)

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

var libsRe = func() *regexp.Regexp {
	libs := strings.Join([]string{
		regexp.QuoteMeta("linux-vdso.so."),
		regexp.QuoteMeta("librt.so."),
		regexp.QuoteMeta("libpthread.so."),
		regexp.QuoteMeta("libdl.so."),
		regexp.QuoteMeta("libm.so."),
		regexp.QuoteMeta("libc.so."),
		strings.Replace(regexp.QuoteMeta("ld-linux-ARCH.so."), "ARCH", ".*", -1),
	}, "|")
	return regexp.MustCompile(libs)
}()

var osVersionRe = regexp.MustCompile(`\d+(\.\d+)*-`)

var isRelease = flag.Bool("release", false, "build in release mode instead of bleeding-edge mode")
var destBucket = flag.String("bucket", "", "override default bucket")
var doProvisional = flag.Bool("provisional", false, "publish provisional binaries")
var doBless = flag.Bool("bless", false, "bless provisional binaries")

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

	// TODO(dan): non-release builds currently aren't broken into the two
	// phases. Instead, the provisional phase does them both.
	if !*isRelease {
		*doProvisional = true
		*doBless = false
	}

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
	var updateLatest bool
	if *isRelease {
		// If the tag starts with "provisional_", then we're building a binary
		// that we hope will be some final release and the tag will be of the
		// form `provisional_<yyyymmddhhss>_<semver>`. If all goes well with the
		// long running tests, these bits will be released exactly as-is, so the
		// version is set to <semver> by stripping the prefix.
		versionStr = provisionalReleasePrefixRE.ReplaceAllLiteralString(branch, "")

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
		cmd := exec.Command("git", "rev-parse", "HEAD")
		cmd.Dir = pkg.Dir
		log.Printf("%s %s", cmd.Env, cmd.Args)
		out, err := cmd.Output()
		if err != nil {
			log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
		}
		versionStr = string(bytes.TrimSpace(out))
		updateLatest = true
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

	var cockroachBuildOpts []opts
	for _, target := range []struct {
		buildType string
		suffix    string
	}{
		// TODO(tamird): consider shifting this information into the builder
		// image; it's conceivable that we'll want to target multiple versions
		// of a given triple.
		{buildType: "darwin", suffix: ".darwin-10.9-amd64"},
		{buildType: "linux-gnu", suffix: ".linux-2.6.32-gnu-amd64"},
		{buildType: "linux-musl", suffix: ".linux-2.6.32-musl-amd64"},
		{buildType: "windows", suffix: ".windows-6.2-amd64.exe"},
	} {
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
			o.PkgDir = pkg.Dir
			o.Branch = branch
			o.VersionStr = versionStr
			o.BucketName = bucketName
			o.BuildType = target.buildType
			o.GoFlags = extraArgs.goflags
			o.Suffix = extraArgs.suffix + target.suffix
			o.Tags = extraArgs.tags
			o.Base = "cockroach" + o.Suffix

			// TODO(tamird): build deadlock,race binaries for all targets?
			if i > 0 && (*isRelease || !strings.HasSuffix(o.BuildType, "linux-gnu")) {
				log.Printf("skipping auxiliary build: %s", pretty.Sprint(o))
				continue
			}
			// race doesn't work without glibc on Linux. See
			// https://github.com/golang/go/issues/14481.
			if strings.HasSuffix(o.BuildType, "linux-musl") && strings.Contains(o.GoFlags, "-race") {
				log.Printf("skipping race build for this configuration")
				continue
			}

			cockroachBuildOpts = append(cockroachBuildOpts, o)
		}
	}

	if *doProvisional {
		for _, o := range cockroachBuildOpts {
			buildCockroach(svc, o)

			absolutePath := filepath.Join(o.PkgDir, o.Base)
			binary, err := os.Open(absolutePath)
			if err != nil {
				log.Fatalf("os.Open(%s): %s", absolutePath, err)
			}
			if !*isRelease {
				putNonRelease(svc, o, binary)
			} else {
				putRelease(svc, o, binary)
			}
			if err := binary.Close(); err != nil {
				log.Fatal(err)
			}
		}
	}
	if *doBless {
		if !*isRelease {
			log.Fatal("cannot bless non-release versions")
		}
		// TODO(dan): It's unfortunate to be doing this inside bless. See if we
		// can split it up like the binaries.
		buildAndPutArchive(svc, opts{
			PkgDir:     pkg.Dir,
			BucketName: bucketName,
			VersionStr: versionStr,
		}, versionStr)
		if updateLatest {
			buildAndPutArchive(svc, opts{
				PkgDir:     pkg.Dir,
				BucketName: bucketName,
				VersionStr: versionStr,
			}, latestStr)
			for _, o := range cockroachBuildOpts {
				markLatestRelease(svc, o)
			}
		}
	}
}

func buildAndPutArchive(svc s3putter, o opts, releaseVersionStr string) {
	log.Printf("building archive %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building archive: %s", pretty.Sprint(o))
	}()

	archiveBase := fmt.Sprintf("cockroach-%s", releaseVersionStr)
	srcArchive := fmt.Sprintf("%s.%s", archiveBase, "src.tgz")
	cmd := exec.Command(
		"make",
		"archive",
		fmt.Sprintf("ARCHIVE_BASE=%s", archiveBase),
		fmt.Sprintf("ARCHIVE=%s", srcArchive),
		fmt.Sprintf("BUILDINFO_TAG=%s", o.VersionStr),
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
	log.Printf("Uploading to s3://%s/%s", o.BucketName, srcArchive)
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

func buildCockroach(svc s3putter, o opts) {
	log.Printf("building cockroach %s", pretty.Sprint(o))
	defer func() {
		log.Printf("done building cockroach: %s", pretty.Sprint(o))
	}()

	{
		args := []string{o.BuildType}
		args = append(args, fmt.Sprintf("%s=%s", "GOFLAGS", o.GoFlags))
		args = append(args, fmt.Sprintf("%s=%s", "SUFFIX", o.Suffix))
		args = append(args, fmt.Sprintf("%s=%s", "TAGS", o.Tags))
		args = append(args, fmt.Sprintf("%s=%s", "BUILDCHANNEL", "official-binary"))
		if *isRelease {
			args = append(args, fmt.Sprintf("%s=%s", "BUILDINFO_TAG", o.VersionStr))
			args = append(args, fmt.Sprintf("%s=%s", "BUILD_TAGGED_RELEASE", "true"))
		}
		cmd := exec.Command("mkrelease", args...)
		cmd.Dir = o.PkgDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		if err := cmd.Run(); err != nil {
			log.Fatalf("%s: %s", cmd.Args, err)
		}
	}

	if strings.Contains(o.BuildType, "linux") {
		binaryName := "./cockroach" + o.Suffix

		cmd := exec.Command(binaryName, "version")
		cmd.Dir = o.PkgDir
		cmd.Env = append(cmd.Env, "MALLOC_CONF=prof:true")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		if err := cmd.Run(); err != nil {
			log.Fatalf("%s %s: %s", cmd.Env, cmd.Args, err)
		}

		// ldd only works on binaries built for the host. "linux-musl"
		// produces fully static binaries, which cause ldd to exit
		// non-zero.
		//
		// TODO(tamird): implement this for all targets.
		if !strings.HasSuffix(o.BuildType, "linux-musl") {
			cmd := exec.Command("ldd", binaryName)
			cmd.Dir = o.PkgDir
			log.Printf("%s %s", cmd.Env, cmd.Args)
			out, err := cmd.Output()
			if err != nil {
				log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
			}
			scanner := bufio.NewScanner(bytes.NewReader(out))
			for scanner.Scan() {
				if line := scanner.Text(); !libsRe.MatchString(line) {
					log.Fatalf("%s is not properly statically linked:\n%s", binaryName, out)
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal(err)
			}
		}
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

func putNonRelease(svc s3putter, o opts, binary io.ReadSeeker) {
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
		Bucket:       &o.BucketName,
		CacheControl: &noCache,
		Key:          &latestKey,
		WebsiteRedirectLocation: &versionKey,
	}); err != nil {
		log.Fatalf("s3 redirect to %s: %s", versionKey, err)
	}
}

func releaseS3Key(o opts) (string, string) {
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

func putRelease(svc s3putter, o opts, binary *os.File) {
	// Stat the binary. Info is needed for archive headers.
	binaryInfo, err := binary.Stat()
	if err != nil {
		log.Fatal(err)
	}

	targetArchiveBase, targetArchive := releaseS3Key(o)
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

func markLatestRelease(svc s3putter, o opts) {
	_, keyRelease := releaseS3Key(o)
	binaryURL := fmt.Sprintf("https://s3.amazonaws.com/%s/%s", o.BucketName, keyRelease)
	log.Printf("Downloading from %s", binaryURL)
	binary, err := http.DefaultClient.Get(binaryURL)
	if err != nil {
		log.Fatalf("downloading %s: %s", binaryURL, err)
	}
	defer binary.Body.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, binary.Body); err != nil {
		log.Fatalf("downloading %s: %s", binaryURL, err)
	}

	oLatest := o
	oLatest.VersionStr = latestStr
	_, keyLatest := releaseS3Key(oLatest)
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
