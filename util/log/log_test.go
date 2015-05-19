// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf

package log

import (
	"bytes"
	"testing"
)

func TestLogKV(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	kvs := []interface{}{"test", 5, error(nil), "test\t\n", struct{}{}, struct{}{}}
	printStructured(buf, kvs)
	exp := []byte("test=\"5\" <nil>=\"test\\t\\n\" {}=\"{}\"\n")
	act := buf.Bytes()
	if !bytes.Equal(exp, act) {
		t.Fatalf("expected %s, got %s", string(exp), string(act))
	}
}
