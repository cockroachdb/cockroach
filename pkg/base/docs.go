// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base

import "github.com/cockroachdb/cockroach/pkg/build"

// DocsURLBase is the root URL for the version of the docs associated with this
// binary.
var DocsURLBase = "https://www.cockroachlabs.com/docs/" + build.VersionPrefix()

// DocsURL generates the URL to pageName in the version of the docs associated
// with this binary.
func DocsURL(pageName string) string { return DocsURLBase + "/" + pageName }
