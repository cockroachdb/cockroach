// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"sort"

	"github.com/cockroachdb/ttycolor"
)

type logFormatter interface {
	formatterName() string
	// doc is used to generate the formatter documentation.
	doc() string
	// formatEntry formats a logEntry into a newly allocated *buffer.
	// The caller is responsible for calling putBuffer() afterwards.
	formatEntry(entry logEntry) *buffer

	// setOption configures the formatter with the given option.
	setOption(key string, value string) error

	// contentType is the MIME content-type field to use on
	// transports which use this metadata.
	contentType() string
}

// FormatParsers maps the user facing format names to the internal representation.
var FormatParsers = map[string]string{
	"crdb-v1":             "v1",
	"crdb-v1-count":       "v1",
	"crdb-v1-tty":         "v1",
	"crdb-v1-tty-count":   "v1",
	"crdb-v2":             "v2",
	"crdb-v2-tty":         "v2",
	"json":                "json",
	"json-compact":        "json-compact",
	"json-fluent":         "json",
	"json-fluent-compact": "json-compact",
}

var formatters = func() map[string]func() logFormatter {
	m := make(map[string]func() logFormatter)
	r := func(f func() logFormatter) {
		name := f().formatterName()
		if _, ok := m[name]; ok {
			panic("duplicate formatter name: " + name)
		}
		m[name] = f
	}
	r(func() logFormatter {
		return &formatCrdbV1{showCounter: false, colorProfile: ttycolor.StderrProfile, colorProfileName: "auto"}
	})
	r(func() logFormatter {
		return &formatCrdbV1{showCounter: false, colorProfileName: "none"}
	})
	r(func() logFormatter {
		return &formatCrdbV1{showCounter: true, colorProfile: ttycolor.StderrProfile, colorProfileName: "auto"}
	})
	r(func() logFormatter {
		return &formatCrdbV1{showCounter: true, colorProfileName: "none"}
	})
	r(func() logFormatter {
		return &formatCrdbV2{colorProfileName: "none"}
	})
	r(func() logFormatter {
		return &formatCrdbV2{colorProfile: ttycolor.StderrProfile, colorProfileName: "auto"}
	})
	r(func() logFormatter { return &formatJSONFull{fluentTag: true, tags: tagCompact} })
	r(func() logFormatter { return &formatJSONFull{fluentTag: true, tags: tagVerbose} })
	r(func() logFormatter { return &formatJSONFull{tags: tagCompact} })
	r(func() logFormatter { return &formatJSONFull{tags: tagVerbose} })
	return m
}()

var formatNames = func() (res []string) {
	for name := range formatters {
		res = append(res, name)
	}
	sort.Strings(res)
	return res
}()

// GetFormatterDocs returns the embedded documentation for all the
// supported formats.
func GetFormatterDocs() map[string]string {
	m := make(map[string]string)
	for fmtName, f := range formatters {
		m[fmtName] = f().doc()
	}
	return m
}
