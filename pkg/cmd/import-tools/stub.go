// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package importtools adds some blank imports in main.go to help in
// `go mod tidy`. See comment in main.go for explanation. As that file is not
// always built, this file exists as a stub to prevent the build from failing
// due to an empty package.
package importtools
