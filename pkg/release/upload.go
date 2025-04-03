// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"
)

// PutReleaseOptions are options to for the PutRelease function.
type PutReleaseOptions struct {
	// NoCache is true if we should set the NoCache option.
	NoCache bool
	// Platform is the platform of the release.
	Platform Platform
	// VersionStr is the version (SHA/branch name) of the release.
	VersionStr string

	ArchivePrefix string
	// OutputDirectory is where to save a copy of the archive
	OutputDirectory string
}

// PutNonReleaseOptions are options to pass into PutNonRelease.
type PutNonReleaseOptions struct {
	// Branch is the branch from which the release is being uploaded from.
	Branch string
	// Files are all the files to be uploaded
	Files []NonReleaseFile
}

// CreateArchive creates a release archive and returns its binary contents.
func CreateArchive(
	platform Platform, version string, prefix string, files []ArchiveFile,
) (bytes.Buffer, error) {
	keys := makeArchiveKeys(platform, version, prefix)
	var body bytes.Buffer

	if strings.HasSuffix(keys.archive, ".zip") {
		if err := createZip(files, &body, keys.base); err != nil {
			return bytes.Buffer{}, fmt.Errorf("cannot create zip %s: %w", keys.archive, err)
		}
	} else {
		if err := createTarball(files, &body, keys.base); err != nil {
			return bytes.Buffer{}, fmt.Errorf("cannot create tarball %s: %w", keys.archive, err)
		}
	}
	return body, nil
}

// PutRelease uploads a compressed archive containing the release
// files and a checksum file of the archive.
func PutRelease(svc ObjectPutGetter, o PutReleaseOptions, body bytes.Buffer) {
	keys := makeArchiveKeys(o.Platform, o.VersionStr, o.ArchivePrefix)
	log.Printf("Uploading to %s", svc.URL(keys.archive))
	// Save local copy
	if o.OutputDirectory != "" {
		localCopy := filepath.Join(o.OutputDirectory, keys.archive)
		dir := filepath.Dir(localCopy)
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("cannot create output directory %s: %s", dir, err)
		}
		log.Printf("saving local copy to %s", localCopy)
		if err := os.WriteFile(localCopy, body.Bytes(), 0644); err != nil {
			log.Fatalf("failed to save a local copy of %s: %s", localCopy, err)
		}
	}
	putObjectInput := PutObjectInput{
		Key:  &keys.archive,
		Body: bytes.NewReader(body.Bytes()),
	}
	if o.NoCache {
		putObjectInput.CacheControl = &NoCache
	}
	if err := svc.PutObject(&putObjectInput); err != nil {
		log.Fatalf("failed uploading %s: %s", keys.archive, err)
	}
	// Generate a SHA256 checksum file with a single entry. Make sure there are 2 spaces in between.
	checksumContents := fmt.Sprintf("%x  %s\n", sha256.Sum256(body.Bytes()),
		filepath.Base(keys.archive))
	targetChecksum := keys.archive + ChecksumSuffix
	if o.OutputDirectory != "" {
		localCopy := filepath.Join(o.OutputDirectory, targetChecksum)
		if err := os.WriteFile(localCopy, []byte(checksumContents), 0644); err != nil {
			log.Fatalf("failed to save a local copy of %s: %s", localCopy, err)
		}
	}
	log.Printf("Uploading to %s", svc.URL(targetChecksum))
	putObjectInputChecksum := PutObjectInput{
		Key:  &targetChecksum,
		Body: strings.NewReader(checksumContents),
	}
	if o.NoCache {
		putObjectInputChecksum.CacheControl = &NoCache
	}
	if err := svc.PutObject(&putObjectInputChecksum); err != nil {
		log.Fatalf("failed uploading %s: %s", targetChecksum, err)
	}
}

func createZip(files []ArchiveFile, body *bytes.Buffer, prefix string) error {
	zw := zip.NewWriter(body)
	for _, f := range files {
		file, err := os.Open(f.LocalAbsolutePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %s", f.LocalAbsolutePath)
		}
		//nolint:deferloop TODO(#137605)
		defer func() { _ = file.Close() }()

		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat: %s", f.LocalAbsolutePath)
		}

		zipHeader, err := zip.FileInfoHeader(stat)
		if err != nil {
			return err
		}
		zipHeader.Name = filepath.Join(prefix, f.ArchiveFilePath)
		zipHeader.Method = zip.Deflate

		zfw, err := zw.CreateHeader(zipHeader)
		if err != nil {
			return err
		}
		if _, err := io.Copy(zfw, file); err != nil {
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return nil
}

func createTarball(files []ArchiveFile, body *bytes.Buffer, prefix string) error {
	gzw := gzip.NewWriter(body)
	tw := tar.NewWriter(gzw)
	for _, f := range files {
		file, err := os.Open(f.LocalAbsolutePath)
		if err != nil {
			return fmt.Errorf("failed to open file: %s", f.LocalAbsolutePath)
		}
		//nolint:deferloop TODO(#137605)
		defer func() { _ = file.Close() }()

		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat: %s", f.LocalAbsolutePath)
		}

		// Set the tar header from the file info. Overwrite name.
		tarHeader, err := tar.FileInfoHeader(stat, "")
		if err != nil {
			return err
		}
		tarHeader.Name = filepath.Join(prefix, f.ArchiveFilePath)
		if err := tw.WriteHeader(tarHeader); err != nil {
			return err
		}

		if _, err := io.Copy(tw, file); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gzw.Close(); err != nil {
		return err
	}
	return nil
}

// PutNonRelease uploads non-release related files.
// Files are uploaded to /cockroach/<FilePath> for each non release file.
// A `latest` key is then put at cockroach/<RedirectPrefix>.<BranchName> that redirects
// to the above file.
func PutNonRelease(svc ObjectPutGetter, o PutNonReleaseOptions) {
	const nonReleasePrefix = "cockroach"
	for _, f := range o.Files {
		disposition := mime.FormatMediaType("attachment", map[string]string{
			"filename": f.FileName,
		})

		fileToUpload, err := os.Open(f.LocalAbsolutePath)
		if err != nil {
			log.Fatalf("failed to open %s: %s", f.LocalAbsolutePath, err)
		}
		//nolint:deferloop TODO(#137605)
		defer func() {
			_ = fileToUpload.Close()
		}()

		versionKey := fmt.Sprintf("%s/%s", nonReleasePrefix, f.FilePath)
		log.Printf("Uploading to %s", svc.URL(versionKey))
		if err := svc.PutObject(&PutObjectInput{
			ContentDisposition: &disposition,
			Key:                &versionKey,
			Body:               fileToUpload,
		}); err != nil {
			log.Fatalf("failed uploading %s: %s", versionKey, err)
		}

		latestSuffix := o.Branch
		if latestSuffix == "master" {
			latestSuffix = "LATEST"
		}
		latestKey := fmt.Sprintf("%s/%s.%s", nonReleasePrefix, f.RedirectPathPrefix, latestSuffix)
		// NB: The leading slash is required to make redirects work
		// correctly since we reuse this key as the redirect location.
		target := "/" + versionKey
		if err := svc.PutObject(&PutObjectInput{
			CacheControl:            &NoCache,
			Key:                     &latestKey,
			WebsiteRedirectLocation: &target,
		}); err != nil {
			log.Fatalf("failed adding a redirect to %s: %s", target, err)
		}
	}
}

type archiveKeys struct {
	base    string
	archive string
}

// makeArchiveKeys extracts the target archive base and archive
// name for the given parameters.
func makeArchiveKeys(platform Platform, versionStr string, archivePrefix string) archiveKeys {
	suffix := SuffixFromPlatform(platform)
	targetSuffix, hasExe := TrimDotExe(suffix)
	if platform == PlatformLinux || platform == PlatformLinuxArm || platform == PlatformLinuxFIPS {
		targetSuffix = strings.Replace(targetSuffix, "gnu-", "", -1)
		targetSuffix = osVersionRe.ReplaceAllLiteralString(targetSuffix, "")
	}
	archiveBase := fmt.Sprintf("%s-%s", archivePrefix, versionStr)
	targetArchiveBase := archiveBase + targetSuffix
	keys := archiveKeys{
		base: targetArchiveBase,
	}
	if hasExe {
		keys.archive = targetArchiveBase + ".zip"
	} else {
		keys.archive = targetArchiveBase + ".tgz"
	}
	return keys
}

const latestStr = "latest"

// LatestOpts are parameters passed to MarkLatestReleaseWithSuffix
type LatestOpts struct {
	Platform   Platform
	VersionStr string
}

// MarkLatestReleaseWithSuffix adds redirects to release files using "latest" instead of the version
func MarkLatestReleaseWithSuffix(svc ObjectPutGetter, o LatestOpts, suffix string) {
	keys := makeArchiveKeys(o.Platform, o.VersionStr, "cockroach")
	versionedKey := "/" + keys.archive + suffix
	oLatest := o
	oLatest.VersionStr = latestStr
	latestKeys := makeArchiveKeys(oLatest.Platform, oLatest.VersionStr, "cockroach")
	latestKey := latestKeys.archive + suffix
	log.Printf("Adding redirect to %s", svc.URL(latestKey))
	if err := svc.PutObject(&PutObjectInput{
		CacheControl:            &NoCache,
		Key:                     &latestKey,
		WebsiteRedirectLocation: &versionedKey,
	}); err != nil {
		log.Fatalf("failed adding a redirect to %s: %s", versionedKey, err)
	}
}

// GetObjectInput specifies input parameters for GetOject
type GetObjectInput struct {
	Key *string
}

// GetObjectOutput specifies output parameters for GetOject
type GetObjectOutput struct {
	Body io.ReadCloser
}

// PutObjectInput specifies input parameters for PutOject
type PutObjectInput struct {
	Key                     *string
	Body                    io.ReadSeeker
	CacheControl            *string
	ContentDisposition      *string
	WebsiteRedirectLocation *string
}

// ObjectPutGetter specifies a minimal interface for cloud storage providers
type ObjectPutGetter interface {
	GetObject(*GetObjectInput) (*GetObjectOutput, error)
	PutObject(*PutObjectInput) error
	Bucket() string
	URL(string) string
}
