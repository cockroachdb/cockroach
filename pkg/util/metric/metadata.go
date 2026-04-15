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

// NewMetadata creates a Metadata with the four fields that almost every
// metric needs, and automatically sets SourceFile to the caller's
// source file. Use this for the common case where no labels, visibility,
// or other optional fields are required.
//
// Usage:
//
//	var myMeta = metric.NewMetadata(
//	    "sql.conns", "Number of connections.",
//	    "Connections", metric.Unit_COUNT,
//	)
func NewMetadata(name, help, measurement string, unit Unit) Metadata {
	return Metadata{
		Name:        name,
		Help:        help,
		Measurement: measurement,
		Unit:        unit,
		SourceFile:  callerSourceFile(1),
	}
}

// InitMetadata returns a copy of m with SourceFile automatically set to
// the caller's source file. Use this when you need to set optional
// fields beyond Name, Help, Measurement, and Unit (e.g. Labels,
// Visibility, Category, HowToUse). For the common case, prefer
// NewMetadata.
//
// Usage:
//
//	var myMeta = metric.InitMetadata(metric.Metadata{
//	    Name:        "sql.conns",
//	    Help:        "Number of connections.",
//	    Measurement: "Connections",
//	    Unit:        metric.Unit_COUNT,
//	    Visibility:  metric.Metadata_ESSENTIAL,
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
	// The function name has the form "path/to/pkg.FuncName" or
	// "path/to/pkg.(*Type).Method". We find the last '/' to isolate
	// the final segment, then take the first '.' after it — that
	// always separates the package name from the symbol.
	pkg := frame.Function
	lastSlash := strings.LastIndex(pkg, "/")
	if dotIdx := strings.Index(pkg[lastSlash+1:], "."); dotIdx >= 0 {
		pkg = pkg[:lastSlash+1+dotIdx]
	}
	pkg = strings.TrimPrefix(pkg, modulePrefix)

	// Join with the filename from the frame.
	return filepath.ToSlash(filepath.Join(pkg, filepath.Base(frame.File)))
}
