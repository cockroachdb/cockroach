// Copyright 2018 The Cockroach Authors.
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

package io

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// These functions are just trivial delegates to code tested below.
var _ = NewWriter(nil)
var _ = NewReader(nil)

func TestReadWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()

	// Write some data to the file until we see a rate.
	{
		w := NewWriteCloser(f)
		var _ io.WriteCloser = w

		for w.Rate() == 0 {
			if _, err := w.Write(make([]byte, 1024*1024)); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Microsecond)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if !w.w.ctr.IsFrozen() {
			t.Fatal("should have been frozen")
		}
	}

	// Read data until we see rate data.
	{
		f, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}

		rc := NewReadCloser(f)
		var _ io.ReadCloser = rc

		buf := make([]byte, 1024)
		for rc.Rate() == 0 {
			if _, err := rc.Read(buf); err != nil {
				if err == io.EOF {
					t.Fatal("did not see rate before hitting EOF")
				}
				t.Fatal(err)
			}
			time.Sleep(100 * time.Microsecond)
		}
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
		if !rc.r.ctr.IsFrozen() {
			t.Fatal("should have been frozen")
		}
	}

	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	}
}
