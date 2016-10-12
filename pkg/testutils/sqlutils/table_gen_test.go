// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlutils

import (
	"bytes"
	"testing"
)

func TestIntToEnglish(t *testing.T) {
	testCases := []struct {
		val int
		exp string
	}{
		{0, "zero"},
		{1, "one"},
		{2, "two"},
		{3, "three"},
		{456, "four-five-six"},
		{70, "seven-zero"},
		{108, "one-zero-eight"},
		{9901, "nine-nine-zero-one"},
	}
	for _, c := range testCases {
		if res := IntToEnglish(c.val); res != c.exp {
			t.Errorf("expected %s, got %s", c.exp, res)
		}
	}
}

func TestGenValues(t *testing.T) {
	var buf bytes.Buffer
	buf = bytes.Buffer{}
	genValues(&buf, 7, 11, ToRowFn(RowIdxFn, RowModuloFn(3), RowEnglishFn))
	expected := `(7,1,'seven'),(8,2,'eight'),(9,0,'nine'),(10,1,'one-zero'),(11,2,'one-one')`
	if buf.String() != expected {
		t.Errorf("expected '%s', got '%s'", expected, buf.String())
	}
}
