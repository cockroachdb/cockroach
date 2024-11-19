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
		buf.Write(b)
		t.Logf("%s", buf.String())
		buf.Reset()

		defaultDir := "/default-dir"
		if err := c.Validate(&defaultDir); err != nil {
			fmt.Fprintf(&buf, "ERROR: %v\n", err)
		} else {
			clearExpectedValues(&c)

			b, err := yaml.Marshal(&c)
			if err != nil {
				t.Fatal(err)
			}
			buf.Write(b)
		}
		return buf.String()
	})
}

// clearExpectedValues clears the various "defaults" fields (because
// at this point we only care that the default values have been
// propagated) and nils out pointers that point to the expect value
// for their field, making the test more concise and future-proof
// without losing information (since if the field was already nil,
// we'd panic dereferencing it, failing the test).  New fields with
// defaults can be added here to avoid having to update each test case.
func clearExpectedValues(c *Config) {
	// clear the default fields to reduce test over-specification
	c.FileDefaults = FileDefaults{}
	c.FluentDefaults = FluentDefaults{}
	c.HTTPDefaults = HTTPDefaults{}

	for _, f := range c.Sinks.FileGroups {
		if *f.Dir == "/default-dir" {
			f.Dir = nil
		}
		if *f.MaxFileSize == ByteSize(10<<20) {
			f.MaxFileSize = nil
		}
		if *f.MaxGroupSize == ByteSize(100<<20) {
			f.MaxGroupSize = nil
		}
		if *f.FilePermissions == DefaultFilePerms {
			f.FilePermissions = nil
		}
		if *f.BufferedWrites == true {
			f.BufferedWrites = nil
		}
		if *f.Format == "crdb-v2" {
			f.Format = nil
		}
		if *f.Redact == false {
			f.Redact = nil
		}
		if *f.Redactable == true {
			f.Redactable = nil
		}
		if *f.Criticality == true {
			f.Criticality = nil
		}
		if f.Buffering.IsNone() {
			f.Buffering = CommonBufferSinkConfigWrapper{}
		}
	}

	// Clear stderr sink defaults
	{
		s := &c.Sinks.Stderr
		if *s.Format == "crdb-v2-tty" {
			s.Format = nil
		}
		if *s.Redact == false {
			s.Redact = nil
		}
		if *s.Redactable == true {
			s.Redactable = nil
		}
		if *s.Criticality == true {
			s.Criticality = nil
		}
		if s.Buffering.IsNone() {
			s.Buffering = CommonBufferSinkConfigWrapper{}
		}
	}
}
