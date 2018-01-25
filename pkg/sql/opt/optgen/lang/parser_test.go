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

package lang

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
)

func TestParser(t *testing.T) {
	testutils.RunDataDrivenTest(t, "testdata/parser", func(d *testutils.TestData) string {
		// Only parse command supported.
		if d.Cmd != "parse" {
			t.FailNow()
		}

		p := NewParser("test.opt")
		p.openFile = func(name string) (io.ReadCloser, error) {
			return &testReader{Reader: strings.NewReader(d.Input)}, nil
		}

		var actual string
		root := p.Parse()
		if root != nil {
			actual = root.String() + "\n"
		} else {
			// Concatenate errors.
			for _, err := range p.Errors() {
				actual = fmt.Sprintf("%s%s\n", actual, err.Error())
			}
		}

		return actual
	})
}

type testReader struct {
	*strings.Reader
}

func (r *testReader) Close() error {
	return nil
}
