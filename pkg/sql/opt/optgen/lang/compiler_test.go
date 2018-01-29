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
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/datadriven"
)

func TestCompiler(t *testing.T) {
	datadriven.RunTest(t, "testdata/compiler", func(d *datadriven.TestData) string {
		// Only compile command supported.
		if d.Cmd != "compile" {
			t.FailNow()
		}

		c := NewCompiler("test.opt")
		c.SetFileResolver(func(name string) (io.Reader, error) {
			return strings.NewReader(d.Input), nil
		})

		var actual string
		compiled := c.Compile()
		if compiled != nil {
			actual = compiled.String()
		} else {
			// Concatenate errors.
			for _, err := range c.Errors() {
				actual = fmt.Sprintf("%s%s\n", actual, err.Error())
			}
		}

		return actual
	})
}

// Test input file not found.
func TestCompilerFileNotFound(t *testing.T) {
	c := NewCompiler("test.opt")
	c.SetFileResolver(func(name string) (io.Reader, error) {
		return nil, errors.New("file not found")
	})

	if compiled := c.Compile(); compiled != nil {
		t.Error("expected nil from Compile")
	}

	if len(c.Errors()) != 1 || c.Errors()[0].Error() != "file not found" {
		t.Errorf("expected error, found: %v", c.Errors())
	}
}

// Test no input files.
func TestCompilerNoFiles(t *testing.T) {
	c := NewCompiler()

	if compiled := c.Compile(); compiled == nil {
		t.Errorf("expected empty result, found nil")
	}
}

// Test multiple input files.
func TestCompilerMultipleFiles(t *testing.T) {
	c := NewCompiler("test.opt", "test2.opt")
	c.SetFileResolver(func(name string) (io.Reader, error) {
		if name == "test.opt" {
			return strings.NewReader("define Foo {}"), nil
		}
		return strings.NewReader("define Bar {}"), nil
	})

	if compiled := c.Compile(); compiled == nil || len(compiled.Defines) != 2 {
		t.Errorf("expected compiled result with two defines, found: %v", compiled)
	}
}

// Test multiple input files with errors.
func TestCompilerMultipleErrorFiles(t *testing.T) {
	c := NewCompiler("path/test.opt", "test2.opt")
	c.SetFileResolver(func(name string) (io.Reader, error) {
		if name == "path/test.opt" {
			return strings.NewReader("define Bar {} define Bar {}"), nil
		}
		return strings.NewReader("[Rule] (Unknown) => (Unknown)"), nil
	})

	if compiled := c.Compile(); compiled != nil {
		t.Error("expected nil result from Compile")
	}

	if len(c.Errors()) != 2 {
		t.Errorf("expected two errors, found: %v", c.Errors())
	}

	if c.Errors()[0].Error() != "test.opt:1:15: duplicate 'Bar' define statement" {
		t.Errorf("expected error, found: %v", c.Errors()[0])
	}

	if c.Errors()[1].Error() != "test2.opt:1:8: unrecognized match name 'Unknown'" {
		t.Errorf("expected error, found: %v", c.Errors()[1])
	}
}
