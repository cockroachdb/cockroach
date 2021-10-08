// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package importtools adds some blank imports in main.go to help in
// `go mod tidy`. See comment in main.go for explanation. As that file is not
// always built, this file exists as a stub to prevent the build from failing
// due to an empty package.
package importtools
