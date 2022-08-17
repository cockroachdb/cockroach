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
	"crypto/sha256"
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
	bazelutil "github.com/cockroachdb/cockroach/pkg/build/util"
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

// Platform is an enumeration of the supported platforms for release.
type Platform int

const (
	// PlatformLinux is the Linux x86_64 target.
	PlatformLinux Platform = iota
	// PlatformLinuxArm is the Linux aarch64 target.
	PlatformLinuxArm
	// PlatformMacOS is the Darwin x86_64 target.
	PlatformMacOS
	// PlatformWindows is the Windows (mingw) x86_64 target.
	PlatformWindows
)

// ChecksumSuffix is a suffix of release tarball checksums
const ChecksumSuffix = ".sha256sum"

// ExecFn is a mockable wrapper for executing commands. The zero value
// ExecFn executes the command normally, but a mock function can be substituted
// in as well.
type ExecFn struct {
	MockExecFn func(*exec.Cmd) ([]byte, error)
}

// Run runs the given Cmd or invokes the MockExecFn as appropriate.
func (e ExecFn) Run(cmd *exec.Cmd) ([]byte, error) {
	if cmd.Stdout != nil {
		return nil, errors.New("exec: Stdout already set")
	}
	var stdout bytes.Buffer
	cmd.Stdout = io.MultiWriter(&stdout, os.Stdout)
	if e.MockExecFn == nil {
		err := cmd.Run()
		return stdout.Bytes(), err
	}
	return e.MockExecFn(cmd)
}

// BuildOptions is a set of options that may be applied to a build.
type BuildOptions struct {
	// True iff this is a release build.
	Release bool
	// BuildTag must be set if Release is set, and vice-versea.
	BuildTag string

	// ExecFn.Run() is called to execute commands for this build.
	// The zero value is appropriate in "real" scenarios but for
	// tests you can update ExecFn.MockExecFn.
	ExecFn ExecFn
}

// SuffixFromPlatform returns the suffix that will be appended to the
// `cockroach` binary when built with the given platform. The binary
// itself can be found in pkgDir/cockroach$SUFFIX after the build.
func SuffixFromPlatform(platform Platform) string {
	switch platform {
	case PlatformLinux:
		return ".linux-2.6.32-gnu-amd64"
	case PlatformLinuxArm:
		return ".linux-3.7.10-gnu-aarch64"
	case PlatformMacOS:
		// TODO(#release): The architecture is at least 10.10 until v20.2 and 10.15 for
		// v21.1 and after. Check whether this can be changed.
		return ".darwin-10.9-amd64"
	case PlatformWindows:
		return ".windows-6.2-amd64.exe"
	default:
		panic(errors.Newf("unknown platform %d", platform))
	}
}

// CrossConfigFromPlatform returns the cross*base config corresponding
// to the given platform. (See .bazelrc for more details.)
func CrossConfigFromPlatform(platform Platform) string {
	switch platform {
	case PlatformLinux:
		return "crosslinuxbase"
	case PlatformLinuxArm:
		return "crosslinuxarmbase"
	case PlatformMacOS:
		return "crossmacosbase"
	case PlatformWindows:
		return "crosswindowsbase"
	default:
		panic(errors.Newf("unknown platform %d", platform))
	}
}

// TargetTripleFromPlatform returns the target triple that will be baked
// into the cockroach binary for the given platform.
func TargetTripleFromPlatform(platform Platform) string {
	switch platform {
	case PlatformLinux:
		return "x86_64-pc-linux-gnu"
	case PlatformLinuxArm:
		return "aarch64-unknown-linux-gnu"
	case PlatformMacOS:
		return "x86_64-apple-darwin19"
	case PlatformWindows:
		return "x86_64-w64-mingw32"
	default:
		panic(errors.Newf("unknown platform %d", platform))
	}
}

// SharedLibraryExtensionFromPlatform returns the shared library extensions for a given Platform.
func SharedLibraryExtensionFromPlatform(platform Platform) string {
	switch platform {
	case PlatformLinux, PlatformLinuxArm:
		return ".so"
	case PlatformWindows:
		return ".dll"
	case PlatformMacOS:
		return ".dylib"
	default:
		panic(errors.Newf("unknown platform %d", platform))
	}
}

// MakeWorkload makes the bin/workload binary.
func MakeWorkload(pkgDir string) error {
	// NB: workload doesn't need anything stamped so we can use `crosslinux`
	// rather than `crosslinuxbase`.
	cmd := exec.Command("bazel", "build", "//pkg/cmd/workload", "--config=crosslinux", "--config=ci")
	cmd.Dir = pkgDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Printf("%s", cmd.Args)
	err := cmd.Run()
	if err != nil {
		return err
	}
	bazelBin, err := getPathToBazelBin(ExecFn{}, pkgDir, []string{"--config=crosslinux", "--config=ci"})
	if err != nil {
		return err
	}
	return stageBinary("//pkg/cmd/workload", PlatformLinux, bazelBin, filepath.Join(pkgDir, "bin"))
}

// MakeRelease makes the release binary and associated files.
func MakeRelease(platform Platform, opts BuildOptions, pkgDir string) error {
	buildArgs := []string{"build", "//pkg/cmd/cockroach", "//c-deps:libgeos", "//pkg/cmd/cockroach-sql"}
	targetTriple := TargetTripleFromPlatform(platform)
	if opts.Release {
		if opts.BuildTag == "" {
			return errors.Newf("must set BuildTag if Release is set")
		}
		buildArgs = append(buildArgs, fmt.Sprintf("--workspace_status_command=./build/bazelutil/stamp.sh %s official-binary %s release", targetTriple, opts.BuildTag))
	} else {
		if opts.BuildTag != "" {
			return errors.Newf("cannot set BuildTag if Release is not set")
		}
		buildArgs = append(buildArgs, fmt.Sprintf("--workspace_status_command=./build/bazelutil/stamp.sh %s official-binary", targetTriple))
	}
	configs := []string{"-c", "opt", "--config=ci", "--config=with_ui", fmt.Sprintf("--config=%s", CrossConfigFromPlatform(platform))}
	buildArgs = append(buildArgs, configs...)
	cmd := exec.Command("bazel", buildArgs...)
	cmd.Dir = pkgDir
	cmd.Stderr = os.Stderr
	log.Printf("%s", cmd.Args)
	stdoutBytes, err := opts.ExecFn.Run(cmd)
	if err != nil {
		return errors.Wrapf(err, "failed to run %s: %s", cmd.Args, string(stdoutBytes))
	}

	// Stage binaries from bazel-bin.
	bazelBin, err := getPathToBazelBin(opts.ExecFn, pkgDir, configs)
	if err != nil {
		return err
	}
	if err := stageBinary("//pkg/cmd/cockroach", platform, bazelBin, pkgDir); err != nil {
		return err
	}
	// TODO: strip the bianry
	if err := stageBinary("//pkg/cmd/cockroach-sql", platform, bazelBin, pkgDir); err != nil {
		return err
	}
	if err := stageLibraries(platform, bazelBin, filepath.Join(pkgDir, "lib")); err != nil {
		return err
	}

	if platform == PlatformLinux {
		suffix := SuffixFromPlatform(platform)
		binaryName := "./cockroach" + suffix

		cmd := exec.Command(binaryName, "version")
		cmd.Dir = pkgDir
		cmd.Env = append(cmd.Env, "MALLOC_CONF=prof:true")
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		stdoutBytes, err := opts.ExecFn.Run(cmd)
		if err != nil {
			return errors.Wrapf(err, "%s %s: %s", cmd.Env, cmd.Args, string(stdoutBytes))
		}

		cmd = exec.Command("ldd", binaryName)
		cmd.Dir = pkgDir
		cmd.Stderr = os.Stderr
		log.Printf("%s %s", cmd.Env, cmd.Args)
		stdoutBytes, err = opts.ExecFn.Run(cmd)
		if err != nil {
			log.Fatalf("%s %s: out=%s err=%v", cmd.Env, cmd.Args, string(stdoutBytes), err)
		}
		scanner := bufio.NewScanner(bytes.NewReader(stdoutBytes))
		for scanner.Scan() {
			if line := scanner.Text(); !linuxStaticLibsRe.MatchString(line) {
				return errors.Newf("%s is not properly statically linked:\n%s", binaryName, line)
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
func S3KeyRelease(platform Platform, versionStr string, binaryPrefix string) (string, string) {
	suffix := SuffixFromPlatform(platform)
	targetSuffix, hasExe := TrimDotExe(suffix)
	// TODO(tamird): remove this weirdness. Requires updating
	// "users" e.g. docs, cockroachdb/cockroach-go, maybe others.
	if platform == PlatformLinux {
		targetSuffix = strings.Replace(targetSuffix, "gnu-", "", -1)
		targetSuffix = osVersionRe.ReplaceAllLiteralString(targetSuffix, "")
	}

	archiveBase := fmt.Sprintf("%s-%s", binaryPrefix, versionStr)
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
func MakeCRDBBinaryNonReleaseFile(localAbsolutePath string, versionStr string) NonReleaseFile {
	base := filepath.Base(localAbsolutePath)
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

// CRDBSharedLibraries are all the shared libraries for CRDB, without the extension.
var CRDBSharedLibraries = []string{"libgeos", "libgeos_c"}

// MakeCRDBLibraryNonReleaseFiles creates the NonReleaseFile objects for relevant
// CRDB shipped libraries.
func MakeCRDBLibraryNonReleaseFiles(
	localAbsoluteBasePath string, platform Platform, versionStr string,
) []NonReleaseFile {
	files := []NonReleaseFile{}
	suffix := SuffixFromPlatform(platform)
	ext := SharedLibraryExtensionFromPlatform(platform)
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
func MakeCRDBBinaryArchiveFile(localAbsolutePath string, path string) ArchiveFile {
	base := filepath.Base(localAbsolutePath)
	_, hasExe := TrimDotExe(base)
	if hasExe {
		path += ".exe"
	}
	return ArchiveFile{
		LocalAbsolutePath: localAbsolutePath,
		ArchiveFilePath:   path,
	}
}

// MakeCRDBLibraryArchiveFiles generates the ArchiveFile object for relevant CRDB helper libraries.
func MakeCRDBLibraryArchiveFiles(pkgDir string, platform Platform) []ArchiveFile {
	files := []ArchiveFile{}
	ext := SharedLibraryExtensionFromPlatform(platform)
	for _, lib := range CRDBSharedLibraries {
		localFileName := lib + ext
		files = append(
			files,
			ArchiveFile{
				LocalAbsolutePath: filepath.Join(pkgDir, "lib", localFileName),
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
	// Platform is the platform of the release.
	Platform Platform
	// VersionStr is the version (SHA/branch name) of the release.
	VersionStr string

	// Files are all the files to be included in the archive.
	Files         []ArchiveFile
	ArchivePrefix string
}

// PutRelease uploads a compressed archive containing the release
// files and a checksum file of the archive to S3.
func PutRelease(svc S3Putter, o PutReleaseOptions) {
	targetArchiveBase, targetArchive := S3KeyRelease(o.Platform, o.VersionStr, o.ArchivePrefix)
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

	log.Printf("Uploading to s3://%s/%s", o.BucketName, targetArchive)
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
	// Generate a SHA256 checksum file with a single entry.
	checksumContents := fmt.Sprintf("%x %s\n", sha256.Sum256(body.Bytes()),
		filepath.Base(targetArchive))
	targetChecksum := targetArchive + ChecksumSuffix
	log.Printf("Uploading to s3://%s/%s", o.BucketName, targetChecksum)
	putObjectInputChecksum := s3.PutObjectInput{
		Bucket: &o.BucketName,
		Key:    &targetChecksum,
		Body:   strings.NewReader(checksumContents),
	}
	if o.NoCache {
		putObjectInputChecksum.CacheControl = &NoCache
	}
	if _, err := svc.PutObject(&putObjectInputChecksum); err != nil {
		log.Fatalf("s3 upload %s: %s", targetChecksum, err)
	}
}

func getPathToBazelBin(execFn ExecFn, pkgDir string, configArgs []string) (string, error) {
	args := []string{"info", "bazel-bin"}
	args = append(args, configArgs...)
	cmd := exec.Command("bazel", args...)
	cmd.Dir = pkgDir
	cmd.Stderr = os.Stderr
	stdoutBytes, err := execFn.Run(cmd)
	if err != nil {
		return "", errors.Wrapf(err, "failed to run %s: %s", cmd.Args, string(stdoutBytes))
	}
	return strings.TrimSpace(string(stdoutBytes)), nil
}

func stageBinary(target string, platform Platform, bazelBin string, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	rel := bazelutil.OutputOfBinaryRule(target, platform == PlatformWindows)
	src := filepath.Join(bazelBin, rel)
	dstBase, _ := TrimDotExe(filepath.Base(rel))
	suffix := SuffixFromPlatform(platform)
	dstBase = dstBase + suffix
	dst := filepath.Join(dir, dstBase)
	srcF, err := os.Open(src)
	if err != nil {
		return err
	}
	defer closeFileOrPanic(srcF)
	dstF, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer closeFileOrPanic(dstF)
	_, err = io.Copy(dstF, srcF)
	return err
}

func stageLibraries(platform Platform, bazelBin string, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	ext := SharedLibraryExtensionFromPlatform(platform)
	for _, lib := range CRDBSharedLibraries {
		libDir := "lib"
		if platform == PlatformWindows {
			// NB: On Windows these libs end up in the `bin` subdir.
			libDir = "bin"
		}
		src := filepath.Join(bazelBin, "c-deps", "libgeos", libDir, lib+ext)
		srcF, err := os.Open(src)
		if err != nil {
			return err
		}
		defer closeFileOrPanic(srcF)
		dst := filepath.Join(dir, filepath.Base(src))
		dstF, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer closeFileOrPanic(dstF)
		_, err = io.Copy(dstF, srcF)
		if err != nil {
			return err
		}
	}
	return nil
}

func closeFileOrPanic(f io.Closer) {
	err := f.Close()
	if err != nil {
		panic(errors.Wrapf(err, "could not close file"))
	}
}
