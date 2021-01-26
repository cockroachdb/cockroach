// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

type logFormatter interface {
	formatterName() string
	// doc is used to generate the formatter documentation.
	doc() string
	// formatEntry formats a logEntry into a newly allocated *buffer.
	// The caller is responsible for calling putBuffer() afterwards.
	formatEntry(entry logEntry) *buffer
}

var formatters = func() map[string]logFormatter {
	m := make(map[string]logFormatter)
	r := func(f logFormatter) {
		m[f.formatterName()] = f
	}
	r(formatCrdbV1{})
	r(formatCrdbV1WithCounter{})
	r(formatCrdbV1TTY{})
	r(formatCrdbV1TTYWithCounter{})
	r(formatCrdbV2{})
	r(formatCrdbV2TTY{})
	r(formatFluentJSONCompact{})
	r(formatFluentJSONFull{})
	r(formatJSONCompact{})
	r(formatJSONFull{})
	return m
}()

// GetFormatterDocs returns the embedded documentation for all the
// supported formats.
func GetFormatterDocs() map[string]string {
	m := make(map[string]string)
	for fmtName, f := range formatters {
		m[fmtName] = f.doc()
	}
	return m
}
