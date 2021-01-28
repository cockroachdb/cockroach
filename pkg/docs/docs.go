// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package docs

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
)

// URLBase is the root URL for the version of the docs associated with this
// binary.
var URLBase = "https://www.cockroachlabs.com/docs/" + build.VersionPrefix()

// URLReleaseNotesBase is the root URL for the release notes for the .0 patch
// release associated with this binary.
var URLReleaseNotesBase = fmt.Sprintf("https://www.cockroachlabs.com/docs/releases/%s.0.html",
	build.VersionPrefix())

// URL generates the URL to pageName in the version of the docs associated
// with this binary.
func URL(pageName string) string { return URLBase + "/" + pageName }

// ReleaseNotesURL generates the URL to pageName in the .0 patch release notes
// docs associated with this binary.
func ReleaseNotesURL(pageName string) string { return URLReleaseNotesBase + pageName }
