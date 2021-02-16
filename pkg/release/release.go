// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package release contains utilities for assisting with the release process.
// This is intended for use for the release commands.
package release

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"mime"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cockroachdb/errors"
)

var (
	// linuxStaticLibsRe returns the regexp of all static libraries.
	linuxStaticLibsRe = func() *regexp.Regexp {
		libs := strings.Join([]string{
			regexp.QuoteMeta("linux-vdso.so."),
			regexp.QuoteMeta("librt.so."),
			regexp.QuoteMeta("libpthread.so."),
			regexp.QuoteMeta("libdl.so."),
			regexp.QuoteMeta("libm.so."),
			regexp.QuoteMeta("libc.so."),
			regexp.QuoteMeta("libresolv.so."),
			strings.Replace(regexp.QuoteMeta("ld-linux-ARCH.so."), "ARCH", ".*", -1),
		}, "|")
		return regexp.MustCompile(libs)
	}()
	osVersionRe = regexp.MustCompile(`\d+(\.\d+)*-`)
)

var (
	// NoCache is a string constant to send no-cache to AWS.
	NoCache = "no-cache"
)

// SupportedTarget contains metadata about a supported target.
type SupportedTarget struct {
	BuildType string
	Suffix    string
}

// SupportedTargets contains the supported targets that we build.
var SupportedTargets = []SupportedTarget{
	{BuildType: "linux-gnu", Suffix: ".linux-2.6.32-gnu-amd64"},
	// TODO(#release): The architecture is at least 10.10 until v20.2 and 10.15 for v21.1 and after.
	// However, this seems to be hardcoded all over the place (in particular, roachprod stage),
	// so keeping the 10.9 standard for now.
	{BuildType: "darwin", Suffix: ".darwin-10.9-amd64"},
	{BuildType: "windows", Suffix: ".windows-6.2-amd64.exe"},
}

// makeReleaseAndVerifyOptions are options for MakeRelease.
type makeReleaseAndVerifyOptions struct {
	args   []string
	env    []string
	execFn ExecFn
}

// ExecFn is a mockable wrapper that executes the given command.
type ExecFn func(*exec.Cmd) ([]byte, error)

// DefaultExecFn is the default exec function.
var DefaultExecFn ExecFn = func(c *exec.Cmd) ([]byte, error) {
	if c.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	var stdout bytes.Buffer
	c.Stdout = io.MultiWriter(&stdout, os.Stdout)
	err := c.Run()
	return stdout.Bytes(), err
}

// MakeReleaseOption as an option for the MakeRelease function.
type MakeReleaseOption func(makeReleaseAndVerifyOptions) makeReleaseAndVerifyOptions

// WithMakeReleaseOptionBuildArg adds a build argument to release.
func WithMakeReleaseOptionBuildArg(arg string) MakeReleaseOption {
	return func(m makeReleaseAndVerifyOptions) makeReleaseAndVerifyOptions {
		m.args = append(m.args, arg)
		return m
	}
}

// WithMakeReleaseOptionExecFn changes the exec function of the given execFn.
func WithMakeReleaseOptionExecFn(r ExecFn) MakeReleaseOption {
	return func(m makeReleaseAndVerifyOptions) makeReleaseAndVerifyOptions {
		m.execFn = r
		return m
	}
}

// WithMakeReleaseOptionEnv adds an environment variable to the build.
func WithMakeReleaseOptionEnv(env string) MakeReleaseOption {
	return func(m makeReleaseAndVerifyOptions) makeReleaseAndVerifyOptions {
		m.env = append(m.env, env)
		return m
	}
}

// MakeWorkload makes the bin/workload binary.
func MakeWorkload(pkgDir string) error {
	cmd := exec.Command("mkrelease", "amd64-linux-gnu", "bin/workload")
	cmd.Dir = pkgDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Printf("%s %s", cmd.Env, cmd.Args)
	return cmd.Run()
}

// MakeRelease makes the release binary and associated files.
func MakeRelease(b SupportedTarget, pkgDir string, opts ...MakeReleaseOption) error {
	params := makeReleaseAndVerifyOptions{
		execFn: DefaultExecFn,
	}
	for _, opt := range opts {
		params = opt(params)
	}

	{
		args := append(
			[]string{b.BuildType, fmt.Sprintf("%s=%s", "SUFFIX", b.Suffix)},
			params.args...,
		)
		cmd := exec.Command("mkrelease", args...)
		cmd.Dir = pkgDir
		cmd.Stderr = os.Stderr
		if len(params.env) > 0 {
			cmd.Env = append(os.Environ(), params.env...)
		}
		log.Printf("%s %s", cmd.Env, cmd.Args)
		if out, err := params.execFn(cmd); err != nil {
			return errors.Newf("%s %s: %s\n\n%s", cmd.Env, cmd.Args, err, out)
		}
	}
	if strings.Contains(b.BuildType, "linux") {
		binaryName := "./cockroach" + b.Suffix

		cmd := exec.Command(binaryName, "version")
		cmd.Dir = pkgDir
		cmd.Env = append(cmd.Env, "MALLOC_CONF=prof:true")
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		if out, err := params.execFn(cmd); err != nil {
			return errors.Newf("%s %s: %s\n\n%s", cmd.Env, cmd.Args, err, out)
		}

		cmd = exec.Command("ldd", binaryName)
		cmd.Dir = pkgDir
		log.Printf("%s %s", cmd.Env, cmd.Args)
		out, err := params.execFn(cmd)
		if err != nil {
			log.Fatalf("%s: out=%q err=%s", cmd.Args, out, err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(out))
		for scanner.Scan() {
			if line := scanner.Text(); !linuxStaticLibsRe.MatchString(line) {
				return errors.Newf("%s is not properly statically linked:\n%s", binaryName, out)
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	return nil
}

// TrimDotExe trims '.exe. from `name` and returns the result (and whether any
// trimming has occurred).
func TrimDotExe(name string) (string, bool) {
	const dotExe = ".exe"
	return strings.TrimSuffix(name, dotExe), strings.HasSuffix(name, dotExe)
}

// S3Putter is an interface allowing uploads to S3.
type S3Putter interface {
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

// S3KeyRelease extracts the target archive base and archive
// name for the given parameters.
func S3KeyRelease(suffix string, buildType string, versionStr string) (string, string) {
	targetSuffix, hasExe := TrimDotExe(suffix)
	// TODO(tamird): remove this weirdness. Requires updating
	// "users" e.g. docs, cockroachdb/cockroach-go, maybe others.
	if strings.Contains(buildType, "linux") {
		targetSuffix = strings.Replace(targetSuffix, "gnu-", "", -1)
		targetSuffix = osVersionRe.ReplaceAllLiteralString(targetSuffix, "")
	}

	archiveBase := fmt.Sprintf("cockroach-%s", versionStr)
	targetArchiveBase := archiveBase + targetSuffix
	if hasExe {
		return targetArchiveBase, targetArchiveBase + ".zip"
	}
	return targetArchiveBase, targetArchiveBase + ".tgz"
}

// NonReleaseFile is a file to upload when publishing a non-release.
type NonReleaseFile struct {
	// S3FileName is the name of the file stored in S3.
	S3FileName string
	// S3FilePath is the path the file should be stored within the  Cockroach bucket.
	S3FilePath string
	// S3RedirectPathPrefix is the prefix of the path that redirects  to the S3FilePath.
	// It is suffixed with .VersionStr or .LATEST, depending on whether  the branch is
	// the master branch.
	S3RedirectPathPrefix string

	// LocalAbsolutePath is the location of the file to upload in the local OS.
	LocalAbsolutePath string
}

// MakeCRDBBinaryNonReleaseFile creates a NonRelease object for the
// CRDB binary.
func MakeCRDBBinaryNonReleaseFile(
	base string, localAbsolutePath string, versionStr string,
) NonReleaseFile {
	remoteName, hasExe := TrimDotExe(base)
	// TODO(tamird): do we want to keep doing this? No longer
	// doing so requires updating cockroachlabs/production, and
	// possibly cockroachdb/cockroach-go.
	remoteName = osVersionRe.ReplaceAllLiteralString(remoteName, "")

	fileName := fmt.Sprintf("%s.%s", remoteName, versionStr)
	if hasExe {
		fileName += ".exe"
	}

	return NonReleaseFile{
		S3FileName:           fileName,
		S3FilePath:           fileName,
		S3RedirectPathPrefix: remoteName,
		LocalAbsolutePath:    localAbsolutePath,
	}
}

// SharedLibraryExtensionFromBuildType returns the extensions for a given buildType.
func SharedLibraryExtensionFromBuildType(buildType string) string {
	switch buildType {
	case "windows":
		return ".dll"
	case "linux-gnu", "linux":
		return ".so"
	case "darwin":
		return ".dylib"
	}
	panic(errors.Newf("unknown build type: %s", buildType))
}

// CRDBSharedLibraries are all the shared libraries for CRDB, without the extension.
var CRDBSharedLibraries = []string{"libgeos", "libgeos_c"}

// MakeCRDBLibraryNonReleaseFiles creates the NonReleaseFile objects for relevant
// CRDB shipped libraries.
func MakeCRDBLibraryNonReleaseFiles(
	localAbsoluteBasePath string, buildType string, versionStr string, suffix string,
) []NonReleaseFile {
	files := []NonReleaseFile{}
	ext := SharedLibraryExtensionFromBuildType(buildType)
	for _, localFileName := range CRDBSharedLibraries {
		remoteFileNameBase, _ := TrimDotExe(osVersionRe.ReplaceAllLiteralString(fmt.Sprintf("%s%s", localFileName, suffix), ""))
		remoteFileName := fmt.Sprintf("%s.%s", remoteFileNameBase, versionStr)

		files = append(
			files,
			NonReleaseFile{
				S3FileName:           fmt.Sprintf("%s%s", remoteFileName, ext),
				S3FilePath:           fmt.Sprintf("lib/%s%s", remoteFileName, ext),
				S3RedirectPathPrefix: fmt.Sprintf("lib/%s%s", remoteFileNameBase, ext),
				LocalAbsolutePath:    filepath.Join(localAbsoluteBasePath, "lib", localFileName+ext),
			},
		)
	}
	return files
}

// PutNonReleaseOptions are options to pass into PutNonRelease.
type PutNonReleaseOptions struct {
	// Branch is the branch from which the release is being uploaded from.
	Branch string
	// BucketName is the bucket to upload the files to.
	BucketName string

	// Files are all the files to be uploaded into S3.
	Files []NonReleaseFile
}

// PutNonRelease uploads non-release related files to S3.
// Files are uploaded to /cockroach/<S3FilePath> for each non release file.
// A latest key is then put at cockroach/<S3RedirectPrefix>.<BranchName> that redirects
// to the above file.
func PutNonRelease(svc S3Putter, o PutNonReleaseOptions) {
	const repoName = "cockroach"
	for _, f := range o.Files {
		disposition := mime.FormatMediaType("attachment", map[string]string{
			"filename": f.S3FileName,
		})

		fileToUpload, err := os.Open(f.LocalAbsolutePath)
		if err != nil {
			log.Fatalf("failed to open %s: %s", f.LocalAbsolutePath, err)
		}
		defer func() {
			_ = fileToUpload.Close()
		}()

		// NB: The leading slash is required to make redirects work
		// correctly since we reuse this key as the redirect location.
		versionKey := fmt.Sprintf("/%s/%s", repoName, f.S3FilePath)
		log.Printf("Uploading to s3://%s%s", o.BucketName, versionKey)
		if _, err := svc.PutObject(&s3.PutObjectInput{
			Bucket:             &o.BucketName,
			ContentDisposition: &disposition,
			Key:                &versionKey,
			Body:               fileToUpload,
		}); err != nil {
			log.Fatalf("s3 upload %s: %s", versionKey, err)
		}

		latestSuffix := o.Branch
		if latestSuffix == "master" {
			latestSuffix = "LATEST"
		}
		latestKey := fmt.Sprintf("%s/%s.%s", repoName, f.S3RedirectPathPrefix, latestSuffix)
		if _, err := svc.PutObject(&s3.PutObjectInput{
			Bucket:                  &o.BucketName,
			CacheControl:            &NoCache,
			Key:                     &latestKey,
			WebsiteRedirectLocation: &versionKey,
		}); err != nil {
			log.Fatalf("s3 redirect to %s: %s", versionKey, err)
		}
	}
}

// ArchiveFile is a file to store in the a archive for a release.
type ArchiveFile struct {
	// LocalAbsolutePath is the location of the file to upload include in the archive on the local OS.
	LocalAbsolutePath string
	// ArchiveFilePath is the location of the file within the archive in which the file is to be stored.
	ArchiveFilePath string
}

// MakeCRDBBinaryArchiveFile generates the ArchiveFile object for a CRDB binary.
func MakeCRDBBinaryArchiveFile(base string, localAbsolutePath string) ArchiveFile {
	_, hasExe := TrimDotExe(base)
	path := "cockroach"
	if hasExe {
		path += ".exe"
	}
	return ArchiveFile{
		LocalAbsolutePath: localAbsolutePath,
		ArchiveFilePath:   path,
	}
}

// MakeCRDBLibraryArchiveFiles generates the ArchiveFile object for relevant CRDB helper libraries.
func MakeCRDBLibraryArchiveFiles(localBasePath string, buildType string) []ArchiveFile {
	files := []ArchiveFile{}
	ext := SharedLibraryExtensionFromBuildType(buildType)
	for _, lib := range CRDBSharedLibraries {
		localFileName := lib + ext
		files = append(
			files,
			ArchiveFile{
				LocalAbsolutePath: filepath.Join(localBasePath, "lib", localFileName),
				ArchiveFilePath:   "lib/" + localFileName,
			},
		)
	}
	return files
}

// PutReleaseOptions are options to for the PutRelease function.
type PutReleaseOptions struct {
	// BucketName is the bucket to upload the files to.
	BucketName string
	// NoCache is true if we should set the NoCache option to S3.
	NoCache bool
	// Suffix is the suffix of the main CRDB binary.
	Suffix string
	// BuildType is the build type of the release.
	BuildType string
	// VersionStr is the version (SHA/branch name) of the release.
	VersionStr string

	// Files are all the files to be included in the archive.
	Files []ArchiveFile
}

// PutRelease uploads a compressed archive containing the release
// files to S3.
func PutRelease(svc S3Putter, o PutReleaseOptions) {
	targetArchiveBase, targetArchive := S3KeyRelease(o.Suffix, o.BuildType, o.VersionStr)
	var body bytes.Buffer

	if strings.HasSuffix(targetArchive, ".zip") {
		zw := zip.NewWriter(&body)

		for _, f := range o.Files {
			file, err := os.Open(f.LocalAbsolutePath)
			if err != nil {
				log.Fatalf("failed to open file: %s", f.LocalAbsolutePath)
			}
			defer func() { _ = file.Close() }()

			stat, err := file.Stat()
			if err != nil {
				log.Fatalf("failed to stat: %s", f.LocalAbsolutePath)
			}

			zipHeader, err := zip.FileInfoHeader(stat)
			if err != nil {
				log.Fatal(err)
			}
			zipHeader.Name = filepath.Join(targetArchiveBase, f.ArchiveFilePath)
			zipHeader.Method = zip.Deflate

			zfw, err := zw.CreateHeader(zipHeader)
			if err != nil {
				log.Fatal(err)
			}
			if _, err := io.Copy(zfw, file); err != nil {
				log.Fatal(err)
			}
		}
		if err := zw.Close(); err != nil {
			log.Fatal(err)
		}
	} else {
		gzw := gzip.NewWriter(&body)
		tw := tar.NewWriter(gzw)
		for _, f := range o.Files {

			file, err := os.Open(f.LocalAbsolutePath)
			if err != nil {
				log.Fatalf("failed to open file: %s", f.LocalAbsolutePath)
			}
			defer func() { _ = file.Close() }()

			stat, err := file.Stat()
			if err != nil {
				log.Fatalf("failed to stat: %s", f.LocalAbsolutePath)
			}

			// Set the tar header from the file info. Overwrite name.
			tarHeader, err := tar.FileInfoHeader(stat, "")
			if err != nil {
				log.Fatal(err)
			}
			tarHeader.Name = filepath.Join(targetArchiveBase, f.ArchiveFilePath)
			if err := tw.WriteHeader(tarHeader); err != nil {
				log.Fatal(err)
			}

			if _, err := io.Copy(tw, file); err != nil {
				log.Fatal(err)
			}
		}
		if err := tw.Close(); err != nil {
			log.Fatal(err)
		}
		if err := gzw.Close(); err != nil {
			log.Fatal(err)
		}
	}

	putObjectInput := s3.PutObjectInput{
		Bucket: &o.BucketName,
		Key:    &targetArchive,
		Body:   bytes.NewReader(body.Bytes()),
	}
	if o.NoCache {
		putObjectInput.CacheControl = &NoCache
	}
	if _, err := svc.PutObject(&putObjectInput); err != nil {
		log.Fatalf("s3 upload %s: %s", targetArchive, err)
	}
}
