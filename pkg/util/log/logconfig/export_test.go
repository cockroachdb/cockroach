// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logconfig

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v2"
)

func TestExport(t *testing.T) {
	datadriven.RunTest(t, "testdata/export", func(t *testing.T, d *datadriven.TestData) string {
		var onlyChans ChannelList
		if d.HasArg("only-channels") {
			var s string
			d.ScanArgs(t, "only-channels", &s)
			chs, err := parseChannelList(s)
			if err != nil {
				t.Fatal(err)
			}
			onlyChans.Channels = chs
		}

		c := DefaultConfig()
		if err := yaml.UnmarshalStrict([]byte(d.Input), &c); err != nil {
			t.Fatal(err)
		}
		defaultDir := "/default-dir"
		var buf bytes.Buffer
		if err := c.Validate(&defaultDir); err != nil {
			t.Fatal(err)
		} else {
			uml, key := c.Export(onlyChans)
			buf.WriteString(uml)
			fmt.Fprintf(&buf, "# http://www.plantuml.com/plantuml/uml/%s\n", key)
		}
		return buf.String()
	})
}
