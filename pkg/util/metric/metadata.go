// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"path/filepath"
	"runtime"
	"strings"
)

const modulePrefix = "github.com/cockroachdb/cockroach/"

// InitMetadata returns a copy of m with SourceFile automatically set to
// the caller's source file. The source file is used at generation time
// to resolve the owning team via CODEOWNERS, eliminating the need for
// a separate AST-scanning step.
//
// Usage:
//
//	var myMeta = metric.InitMetadata(metric.Metadata{
//	    Name:        "sql.conns",
//	    Help:        "Number of connections.",
//	    Measurement: "Connections",
//	    Unit:        metric.Unit_COUNT,
//	})
func InitMetadata(m Metadata) Metadata {
	m.SourceFile = callerSourceFile(1)
	return m
}

// callerSourceFile returns the repo-relative source file of the caller
// at the given depth (0 = callerSourceFile's caller). The path is
// derived from the fully-qualified function name recorded in the
// binary's debug info, which uses the Go import path regardless of
// the physical location on disk. This makes it stable across local
// builds, Bazel sandboxes, and CI.
//
// For example, a call from pkg/kv/kvserver/metrics.go returns
// "pkg/kv/kvserver/metrics.go".
func callerSourceFile(skip int) string {
	var pcs [1]uintptr
	if runtime.Callers(skip+2, pcs[:]) == 0 {
		return ""
	}
	frame, _ := runtime.CallersFrames(pcs[:]).Next()
	if frame.Function == "" {
		return ""
	}

	// Extract the package path from the fully-qualified function name.
	// e.g., "github.com/cockroachdb/cockroach/pkg/kv/kvserver.init"
	// becomes "pkg/kv/kvserver".
	pkg := frame.Function
	if idx := strings.LastIndex(pkg, "."); idx >= 0 {
		pkg = pkg[:idx]
	}
	pkg = strings.TrimPrefix(pkg, modulePrefix)

	// Join with the filename from the frame.
	return filepath.ToSlash(filepath.Join(pkg, filepath.Base(frame.File)))
}
