// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package collatedstring

import "golang.org/x/text/collate"

// DefaultCollationTag is the "default" collation for strings.
const DefaultCollationTag = "default"

// CCollationTag is the "C" collation for strings. At the time of writing
// this, it behaves the same os "default" since LC_COLLATE cannot be modified.
const CCollationTag = "C"

// PosixCollationTag is the "POSIX" collation for strings. At the time of
// writing this, it behaves the same os "default" since LC_COLLATE cannot be
// modified.
const PosixCollationTag = "POSIX"

var supportedTagNames []string

func IsDefaultEquivalentCollation(s string) bool {
	return s == DefaultCollationTag || s == CCollationTag || s == PosixCollationTag
}

// Supported returns a list of all the collation names that are supported.
func Supported() []string {
	return supportedTagNames
}

func init() {
	if collate.CLDRVersion != "23" {
		panic("This binary was built with an incompatible version of golang.org/x/text. " +
			"See https://github.com/cockroachdb/cockroach/issues/63738 for details")
	}

	supportedTagNames = []string{
		DefaultCollationTag,
		CCollationTag,
		PosixCollationTag,
	}
	for _, t := range collate.Supported() {
		supportedTagNames = append(supportedTagNames, t.String())
	}
}
