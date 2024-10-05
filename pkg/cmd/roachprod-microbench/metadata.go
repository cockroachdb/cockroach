// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"archive/tar"
	"io"
	"os"
	"strings"

	"github.com/klauspost/compress/gzip"
)

const binRunSuffix = "/bin/run.sh"

// readArchivePackages reads the entries in the provided archive file and
// returns a list of packages for which test binaries have been built.
func readArchivePackages(archivePath string) ([]string, error) {
	file, err := os.Open(archivePath)
	defer func() { _ = file.Close() }()
	if err != nil {
		return nil, err
	}

	reader := io.Reader(file)
	if strings.HasSuffix(archivePath, ".gz") {
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
	}

	packages := make([]string, 0)
	tarScan := tar.NewReader(reader)
	for {
		entry, nextErr := tarScan.Next()
		if nextErr == io.EOF {
			break
		}
		if nextErr != nil {
			return nil, err
		}

		if strings.HasSuffix(entry.Name, binRunSuffix) {
			packages = append(packages, strings.TrimSuffix(entry.Name, binRunSuffix))
		}
	}
	return packages, nil
}

// getPackagesFromLogs scans a directory for benchmark report logs and
// creates a list of packages that were used to generate the logs.
func getPackagesFromLogs(dir string) ([]string, error) {
	packages := make([]string, 0)
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if isReportLog(file.Name()) {
			packages = append(packages, getPackageFromReportLogName(file.Name()))
		}
	}
	return packages, nil
}
