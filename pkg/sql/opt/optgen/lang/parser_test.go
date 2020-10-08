// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lang

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestParser(t *testing.T) {
	datadriven.RunTest(t, "testdata/parser", func(t *testing.T, d *datadriven.TestData) string {
		// Only parse command supported.
		if d.Cmd != "parse" {
			t.FailNow()
		}

		args := []string{"test.opt"}
		for _, cmdArg := range d.CmdArgs {
			// Add additional args.
			args = append(args, cmdArg.String())
		}

		p := NewParser(args...)
		p.SetFileResolver(func(name string) (io.Reader, error) {
			if name == "test.opt" {
				return strings.NewReader(d.Input), nil
			}
			return nil, fmt.Errorf("unknown file '%s'", name)
		})

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
