// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

// NoLogV returns a verbosity level that will not result in VEvents and
// VErrEvents being logged.
func NoLogV() Level {
	return logging.vmoduleConfig.verbosity.get() + 1
}
