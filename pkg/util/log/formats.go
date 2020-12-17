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
	return m
}()
