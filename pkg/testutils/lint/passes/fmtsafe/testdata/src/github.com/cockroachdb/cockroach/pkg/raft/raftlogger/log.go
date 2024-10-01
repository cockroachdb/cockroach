// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlogger

type Logger interface {
	Info(args ...interface{})
	Infof(format string, args ...interface{})
}
