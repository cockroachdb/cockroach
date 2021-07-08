// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/dev/recorder"
)

// Exec is a convenience wrapper around the stdlib os/exec package. It lets us:
//
// (a) mock all instances where we shell out, for tests, and
// (b) capture all instances of shelling out that take place during execution
//
// We achieve (a) by embedding a Recorder, and either replaying from it if
// configured to do so, or "doing the real thing" and recording the fact into
// the Recorder for future playback.
//
// For (b), each operation is logged (if configured to do so). These messages
// can be captured by the caller and compared against what is expected.
type Exec struct {
	dir            string
	logger         *log.Logger
	stdout, stderr io.Writer
	*recorder.Recorder
}

// New returns a new Exec with the given options.
func New(opts ...Option) *Exec {
	e := &Exec{}

	// Apply the default options.
	defaults := []func(executor *Exec){
		WithLogger(log.New(os.Stdout, "executing: ", 0)),
		WithStdOutErr(os.Stdout, os.Stderr),
	}
	for _, opt := range defaults {
		opt(e)
	}

	// Apply the user-provided options, overriding the defaults as necessary.
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Option is a start-up option that can be specified when constructing a new
// Exec handle.
type Option func(e *Exec)

// WithLogger configures Exec to use the provided logger.
func WithLogger(logger *log.Logger) func(e *Exec) {
	return func(e *Exec) {
		e.logger = logger
	}
}

// WithStdOutErr configures Exec to use the provided sinks for std{out,err}.
func WithStdOutErr(stdout, stderr io.Writer) func(e *Exec) {
	return func(e *Exec) {
		e.stdout = stdout
		e.stderr = stderr
	}
}

// WithRecorder configures Exec to use the provided recorder.
func WithRecorder(r *recorder.Recorder) func(e *Exec) {
	return func(e *Exec) {
		e.Recorder = r
	}
}

// WithWorkingDir configures Exec to use the provided working directory.
func WithWorkingDir(dir string) func(e *Exec) {
	return func(e *Exec) {
		e.dir = dir
	}
}

// CommandContext wraps around exec.CommandContext, executing the named program
// with the given arguments.
func (e *Exec) CommandContext(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := fmt.Sprintf("%s %s", name, strings.Join(args, " "))
	e.logger.Print(command)

	var buffer bytes.Buffer
	if e.Recorder == nil || e.Recorder.Recording() {
		// Do the real thing.
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Stdout = io.MultiWriter(e.stdout, &buffer)
		cmd.Stderr = e.stderr
		cmd.Dir = e.dir

		if err := cmd.Start(); err != nil {
			return nil, err
		}
		if err := cmd.Wait(); err != nil {
			return nil, err
		}
	}

	if e.Recorder == nil {
		return buffer.Bytes(), nil
	}

	if e.Recording() {
		if err := e.record(command, buffer.String()); err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	}

	output, err := e.replay(command)
	if err != nil {
		return nil, err
	}

	return []byte(output), nil
}

// CommandContextSilent is like CommandContext, but does not take over
// stdout/stderr. It's to be used for "internal" operations.
func (e *Exec) CommandContextSilent(
	ctx context.Context, name string, args ...string,
) ([]byte, error) {
	command := fmt.Sprintf("%s %s", name, strings.Join(args, " "))
	e.logger.Print(command)

	var buffer bytes.Buffer
	if e.Recorder == nil || e.Recorder.Recording() {
		// Do the real thing.
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Stdout = &buffer
		cmd.Stderr = ioutil.Discard
		cmd.Dir = e.dir

		if err := cmd.Start(); err != nil {
			return nil, err
		}
		if err := cmd.Wait(); err != nil {
			return nil, err
		}
	}

	if e.Recorder == nil {
		return buffer.Bytes(), nil
	}

	if e.Recording() {
		if err := e.record(command, buffer.String()); err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	}

	output, err := e.replay(command)
	if err != nil {
		return nil, err
	}

	return []byte(output), nil
}

// replay replays the specified command, erroring out if it's mismatched with
// what the recorder plays back next. It returns the recorded output.
func (e *Exec) replay(command string) (output string, err error) {
	found, err := e.Recorder.Next(func(op recorder.Operation) error {
		if op.Command != command {
			return fmt.Errorf("expected %q, got %q", op.Command, command)
		}
		output = op.Output
		return nil
	})
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("recording for %q not found", command)
	}
	return output, nil
}

// record records the specified command with the corresponding output.
func (e *Exec) record(command, output string) error {
	op := recorder.Operation{
		Command: command,
		Output:  output,
	}

	return e.Record(op)
}
