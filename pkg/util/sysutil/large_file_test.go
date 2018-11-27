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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sysutil

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/pkg/errors"
)

func TestLargeFile(t *testing.T) {
	f, err := ioutil.TempFile("", "input")
	if err != nil {
		t.Fatal(err)
	}
	fname := f.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	const n int64 = 1013
	if err := CreateLargeFile(fname, n); err != nil {
		t.Fatal(err)
	}
	s, err := os.Stat(fname)
	if err != nil {
		t.Fatal(err)
	}
	if s.Size() != n {
		t.Fatal(errors.Errorf("expected size of file %d, got %d", n, s.Size()))
	}
}
