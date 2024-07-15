// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
