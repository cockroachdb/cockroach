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

	"github.com/golang/snappy"
)

type snappyCompressor struct {
}

func (snappyCompressor) Do(w io.Writer, p []byte) error {
	z := snappy.NewBufferedWriter(w)
	if _, err := z.Write(p); err != nil {
		return err
	}
	return z.Close()
}

func (snappyCompressor) Type() string {
	return "snappy"
}

type snappyDecompressor struct {
}

func (snappyDecompressor) Do(r io.Reader) ([]byte, error) {
	return ioutil.ReadAll(snappy.NewReader(r))
}

func (snappyDecompressor) Type() string {
	return "snappy"
}
