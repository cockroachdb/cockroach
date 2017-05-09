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

package rpc

import (
	"io"
	"io/ioutil"
	"sync"

	"github.com/golang/snappy"
)

// NB: The grpc.{Compressor,Decompressor} implementations need to be goroutine
// safe as multiple goroutines may be using the same compressor/decompressor
// for different streams on the same connection.
var snappyWriterPool sync.Pool
var snappyReaderPool sync.Pool

type snappyCompressor struct {
}

func (snappyCompressor) Do(w io.Writer, p []byte) error {
	z, ok := snappyWriterPool.Get().(*snappy.Writer)
	if !ok {
		z = snappy.NewBufferedWriter(w)
	} else {
		z.Reset(w)
	}
	_, err := z.Write(p)
	if err == nil {
		err = z.Flush()
	}
	snappyWriterPool.Put(z)
	return err
}

func (snappyCompressor) Type() string {
	return "snappy"
}

type snappyDecompressor struct {
}

func (snappyDecompressor) Do(r io.Reader) ([]byte, error) {
	z, ok := snappyReaderPool.Get().(*snappy.Reader)
	if !ok {
		z = snappy.NewReader(r)
	} else {
		z.Reset(r)
	}
	b, err := ioutil.ReadAll(z)
	snappyReaderPool.Put(z)
	return b, err
}

func (snappyDecompressor) Type() string {
	return "snappy"
}
