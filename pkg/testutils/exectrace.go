// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/trace"

	"github.com/petermattis/goid"
)

type ActiveExecTrace struct {
	name string
	file *os.File
	reg  *trace.Region
}

// Finish stops the ongoing execution trace, if there is one, and closes the
// file. It must be called only once.
func (a *ActiveExecTrace) Finish(t interface {
	Failed() bool
	Logf(string, ...interface{})
}) {
	if a == nil {
		return
	}
	a.reg.End()
	trace.Stop()
	_ = a.file.Close()
	if !t.Failed() {
		_ = os.Remove(a.file.Name())
	} else {
		t.Logf("execution trace written to %s", a.file.Name())
	}
}

// StartExecTrace starts a Go execution trace and returns a handle that allows
// stopping it. If a trace cannot be started, this is logged and nil is returned.
// It is valid to stop a nil ActiveExecTrace.
//
// This helper is intended to instrument tests for which an execution trace is
// desired on the next failure.
func StartExecTrace(
	t interface {
		Name() string
		Logf(string, ...interface{})
	}, dir string,
) *ActiveExecTrace {
	path := filepath.Join(dir, fmt.Sprintf("exectrace_goid_%d.bin", goid.Get()))
	f, err := os.Create(path)
	if err != nil {
		t.Logf("could not create file for execution trace: %s", err)
		return nil
	}
	if err := trace.Start(f); err != nil {
		_ = f.Close()
		t.Logf("could not start execution trace: %s", err)
		return nil
	}
	return &ActiveExecTrace{
		name: t.Name(),
		file: f,
		reg:  trace.StartRegion(context.Background(), t.Name()),
	}
}
