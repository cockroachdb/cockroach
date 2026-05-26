// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"io"

	"github.com/klauspost/pgzip"
)

// compress compresses the input stream and writes it to the output stream. This
// supplies a multithreaded compression algorithm that is faster than the gzip
// available on CI and roachprod nodes.
func compress(w io.Writer, r io.Reader) error {
	writer := pgzip.NewWriter(w)
	defer func() { _ = writer.Close() }()
	_, err := io.Copy(writer, r)
	if err != nil {
		return err
	}
	return nil
}
