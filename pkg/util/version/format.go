// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package version

import "github.com/cockroachdb/redact"

var _ redact.SafeFormatter = MajorVersion{}
var _ redact.SafeFormatter = Version{}

func (m MajorVersion) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Print(m.String())
}

func (v Version) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Print(v.String())
}
