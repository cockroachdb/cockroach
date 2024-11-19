// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
)

// sql-bootstrap-data is a program that is used to generate sql catalog
// bootstrap data files. These files are used to bootstrap a cluster at older
// (but still supported) versions.
//
// To run:
//   ./dev build sql-bootstrap-data
//   ./bin/sql-bootstrap-data

func main() {
	version := clusterversion.Latest.ReleaseSeries()
	systemKeys, systemHash := bootstrap.GetAndHashInitialValuesToString(0)
	// Note: the tenant ID does not matter, as long as it's not 0 (the system
	// tenant); the tenant-specific prefix is stripped out of the keys.
	tenantKeys, tenantHash := bootstrap.GetAndHashInitialValuesToString(12345)

	fmt.Printf("\n// Commit the generated files and " +
		"add the following declarations to the end of initial_values.go:\n")
	writeDataFile(version, "system.keys", systemKeys)
	writeDataFile(version, "system.sha256", systemHash)
	writeDataFile(version, "tenant.keys", tenantKeys)
	writeDataFile(version, "tenant.sha256", tenantHash)

	fmt.Printf("\n\n// Add the entry below to the initialValuesFactoryByKey map:\n")
	fmt.Printf(`
	clusterversion.V%[1]d_%[2]d: hardCodedInitialValues{
		system:        v%[1]d_%[2]d_system_keys,
		systemHash:    v%[1]d_%[2]d_system_sha256,
		nonSystem:     v%[1]d_%[2]d_tenant_keys,
		nonSystemHash: v%[1]d_%[2]d_tenant_sha256,
	}.build,
`, version.Major, version.Minor)

	fmt.Printf("\n\n// Add these to BUILD.bazel, under embedsrcs:\n")
	fmt.Printf(`
        "data/%[1]d_%[2]d_system.keys",
        "data/%[1]d_%[2]d_system.sha256",
        "data/%[1]d_%[2]d_tenant.keys",
        "data/%[1]d_%[2]d_tenant.sha256",
`, version.Major, version.Minor)
}

// writeDataFile creates a file in pkg/sql/catalog/bootstrap/data.
func writeDataFile(version roachpb.ReleaseSeries, filenameSuffix, data string) {
	filename := filepath.Join(
		"pkg", "sql", "catalog", "bootstrap", "data",
		fmt.Sprintf("%d_%d_%s", version.Major, version.Minor, filenameSuffix),
	)
	if err := os.WriteFile(filename, []byte(data), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing %s: %v\n", filename, err)
		os.Exit(1)
	}

	fmt.Printf("\n")
	fmt.Printf("//go:embed data/%d_%d_%s\n", version.Major, version.Minor, filenameSuffix)
	fmt.Printf("var v%d_%d_%s string\n", version.Major, version.Minor, strings.ReplaceAll(filenameSuffix, ".", "_"))
}
