// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import "github.com/cockroachdb/cockroach/pkg/storage"

type StateEngine storage.Engine

type StateReader struct {
	storage.Reader
}

func MakeStateReader(r storage.Reader) StateReader {
	return StateReader{Reader: r}
}

type StateReadWriter struct {
	storage.ReadWriter
}

func MakeStateRW(rw storage.ReadWriter) StateReadWriter {
	return StateReadWriter{ReadWriter: rw}
}

func (rw StateReadWriter) Reader() StateReader {
	return StateReader{Reader: rw.ReadWriter}
}
