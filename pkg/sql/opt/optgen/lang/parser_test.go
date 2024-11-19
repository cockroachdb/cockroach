// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lang

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

func TestParser(t *testing.T) {
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "parser"), func(t *testing.T, d *datadriven.TestData) string {
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
