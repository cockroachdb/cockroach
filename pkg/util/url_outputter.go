// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"fmt"
	"net/url"
)

// URLOutputter handles writing strings into an encoded URL.
// It also ensures that (in the text that is encoded by the URL) each
// entry gets its own line and there's exactly one blank line between entries.
type URLOutputter struct {
	buf bytes.Buffer
}

// Writef adds a formatted string to the outputter's buffer.
func (e *URLOutputter) Writef(format string, args ...interface{}) {
	if e.buf.Len() > 0 {
		e.buf.WriteString("\n")
	}
	fmt.Fprintf(&e.buf, format, args...)
}

// Finish generates a URL that encodes all of the text that's been added with
// Writef, returning an error if something went wrong.
func (e *URLOutputter) Finish() (url.URL, error) {
	// Generate a URL that encodes all the text.
	var compressed bytes.Buffer
	encoder := base64.NewEncoder(base64.URLEncoding, &compressed)
	compressor := zlib.NewWriter(encoder)
	if _, err := e.buf.WriteTo(compressor); err != nil {
		return url.URL{}, err
	}
	if err := compressor.Close(); err != nil {
		return url.URL{}, err
	}
	if err := encoder.Close(); err != nil {
		return url.URL{}, err
	}
	return url.URL{
		Scheme:   "https",
		Host:     "cockroachdb.github.io",
		Path:     "text/decode.html",
		Fragment: compressed.String(),
	}, nil
}
