// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import "os"

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
