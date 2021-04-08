// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package issues

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/datadriven"
)

func TestRenderer(t *testing.T) {
	tcs := map[string]func(*Renderer){
		"escaped": func(r *Renderer) {
			r.Escaped("<p><-- p tag, and linking to #1 which is 'a thing'</p>")
		},
		"escaped-multiline": func(r *Renderer) {
			r.Escaped(`The most important properties of all this are that

- it does not get in the way of things
- it's easy to use and easy to read

Furthermore, it should respect markdown paragraphs and ` + "`" + `inline` + "`" + ` code,
as well as

` + "```\ncode blocks\n```\n\n well.")
		},
		"a-simple": func(r *Renderer) {
			r.A("hello (you)", "https://foo:123/query?amp=goo&slurp=1")
		},
		"a-contrived": func(r *Renderer) {
			r.A("[(two-deep)](one-deep)[one-deep][(imbalanced", "http://!@#$%^&*()/<blink>>><<<brainf*ck")
		},
		"collapsed-simple": func(r *Renderer) {
			r.Collapsed("foo", func() {
				r.CodeBlock("", "bar")
			})
		},
		"collapsed-contrived": func(r *Renderer) {
			r.Collapsed("<some> ``` weird stuff & </a> going on here", func() {
				r.CodeBlock("", "Does this\n``` work?<p><-- should have a paragraph tag")
			})
		},
	}

	dir := filepath.Join("testdata", "render")
	datadriven.Walk(t, dir, func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			if td.Cmd != "render" {
				t.Fatal("unsupported command")
			}
			name := filepath.Base(path)
			render, ok := tcs[name]
			delete(tcs, name)
			if !ok {
				t.Fatalf("testdata %s has no generating test case", name)
			}
			r := &Renderer{}
			render(r)
			act := r.buf.String()
			t.Log(act)
			t.Log(ghURL(t, "testing", act))
			return act
		})
	})
	for name := range tcs {
		path := filepath.Join(dir, name)
		t.Errorf("testdata file missing: %s", path)
		const create = true
		if create {
			_ = ioutil.WriteFile(path, []byte(`render
----
`), 0644)
		}
	}
}
