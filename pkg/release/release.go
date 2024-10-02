// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package release contains utilities for assisting with the release process.
// This is intended for use for the release commands.
package release

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

var (
	// NoCache is a string constant to send no-cache to AWS.
	NoCache = "no-cache"
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

// TrimDotExe trims '.exe. from `name` and returns the result (and whether any
// trimming has occurred).
func TrimDotExe(name string) (string, bool) {
	const dotExe = ".exe"
	return strings.TrimSuffix(name, dotExe), strings.HasSuffix(name, dotExe)
}

// NonReleaseFile is a file to upload when publishing a non-release.
type NonReleaseFile struct {
	// FileName is the name of the file stored in the cloud.
	FileName string
	// FilePath is the path the file should be stored within the  Cockroach bucket.
	FilePath string
	// RedirectPathPrefix is the prefix of the path that redirects  to the FilePath.
	// It is suffixed with .VersionStr or .LATEST, depending on whether  the branch is
	// the master branch.
	RedirectPathPrefix string

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
		FileName:           fileName,
		FilePath:           fileName,
		RedirectPathPrefix: remoteName,
		LocalAbsolutePath:  localAbsolutePath,
	}
}

// CRDBSharedLibraries are all the shared libraries for CRDB, without the extension.
var CRDBSharedLibraries = []string{"libgeos", "libgeos_c"}

// MakeCRDBLibraryNonReleaseFiles creates the NonReleaseFile objects for relevant
// CRDB shipped libraries.
func MakeCRDBLibraryNonReleaseFiles(
	localAbsoluteBasePath string, platform Platform, versionStr string,
) []NonReleaseFile {
	var files []NonReleaseFile
	if platform == PlatformMacOSArm || platform == PlatformWindows {
		return files
	}
	suffix := SuffixFromPlatform(platform)
	ext := SharedLibraryExtensionFromPlatform(platform)
	for _, localFileName := range CRDBSharedLibraries {
		remoteFileNameBase, _ := TrimDotExe(osVersionRe.ReplaceAllLiteralString(fmt.Sprintf("%s%s", localFileName, suffix), ""))
		remoteFileName := fmt.Sprintf("%s.%s", remoteFileNameBase, versionStr)

		files = append(
			files,
			NonReleaseFile{
				FileName:           fmt.Sprintf("%s%s", remoteFileName, ext),
				FilePath:           fmt.Sprintf("lib/%s%s", remoteFileName, ext),
				RedirectPathPrefix: fmt.Sprintf("lib/%s%s", remoteFileNameBase, ext),
				LocalAbsolutePath:  filepath.Join(localAbsoluteBasePath, "lib", localFileName+ext),
			},
		)
	}
	return files
}

// ArchiveFile is a file to store in the archive for a release.
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
	var files []ArchiveFile
	if platform == PlatformMacOSArm || platform == PlatformWindows {
		return files
	}
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
