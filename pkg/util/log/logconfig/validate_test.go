// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logconfig

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v2"
)

func TestValidate(t *testing.T) {
	datadriven.RunTest(t, "testdata/validate", func(t *testing.T, d *datadriven.TestData) string {
		c := DefaultConfig()
		if err := yaml.UnmarshalStrict([]byte(d.Input), &c); err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		fmt.Fprintln(&buf, "## before validate:\n#")
		b, err := yaml.Marshal(&c)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&buf, "%s", string(b))
		t.Logf("%s", buf.String())
		buf.Reset()

		defaultDir := "/default-dir"
		if err := c.Validate(&defaultDir); err != nil {
			fmt.Fprintf(&buf, "ERROR: %v\n", err)
		} else {
			b, err := yaml.Marshal(&c)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(&buf, "%s", string(b))
		}
		return buf.String()
	})
}
