// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
