// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

// A Error wraps an error returned from a RocksDB operation.
type Error struct {
	msg string
}

var _ errors.SafeMessager = (*Error)(nil)

// Error implements the error interface.
func (err *Error) Error() string {
	return err.msg
}

// SafeMessage implements log.SafeMessager. RocksDB errors are not very
// well-structured and we additionally only pass a stringified representation
// from C++ to Go. The error usually takes the form "<typeStr>: [<subtypeStr>]
// <msg>" where `<typeStr>` is generated from an enum and <subtypeStr> is rarely
// used. <msg> usually contains the bulk of information and follows no
// particular rules.
//
// To extract safe messages from these errors, we keep a dictionary generated
// from the RocksDB source code and report verbatim all words from the
// dictionary (masking out the rest which in particular includes paths).
//
// The originating RocksDB error type is defined in
// c-deps/rocksdb/util/status.cc.
func (err Error) SafeMessage() string {
	var out []string
	// NB: we leave (unix and windows style) directory separators in the cleaned
	// string to avoid a directory such as /mnt/rocksdb/known/words from showing
	// up. The dictionary is all [a-zA-Z], so anything that has a separator left
	// within after cleaning is going to be redacted.
	cleanRE := regexp.MustCompile(`[^a-zA-Z\/\\]+`)
	for _, field := range strings.Fields(err.msg) {
		word := strings.ToLower(cleanRE.ReplaceAllLiteralString(field, ""))
		if _, isSafe := rocksDBErrorDict[word]; isSafe {
			out = append(out, word)
		} else {
			out = append(out, "<redacted>")
		}
	}
	return strings.Join(out, " ")
}
