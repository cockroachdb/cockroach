// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package engine

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// A RocksDBError wraps an error returned from a RocksDB operation.
type RocksDBError struct {
	msg string
}

var _ log.SafeMessager = (*RocksDBError)(nil)

// Error implements the error interface.
func (err *RocksDBError) Error() string {
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
func (err RocksDBError) SafeMessage() string {
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
