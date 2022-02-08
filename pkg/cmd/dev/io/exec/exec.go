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
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/alessio/shellescape"
	"github.com/irfansharif/recorder"
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
	blocking       bool
	*recorder.Recorder

	knobs struct { // testing knobs
		dryrun    bool
		intercept map[string]string // maps commands to outputs
	}
}

// New returns a new Exec with the given options.
func New(opts ...Option) *Exec {
	e := &Exec{
		blocking: true,
	}

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

// AsNonBlocking creates a copy of Exec that doesn't block while commands run
// (except when called via CommandContextSilent and CommandContextWithInput).
func (e *Exec) AsNonBlocking() *Exec {
	out := *e
	out.blocking = false
	return &out
}

// WithWorkingDir configures Exec to use the provided working directory.
func WithWorkingDir(dir string) func(e *Exec) {
	return func(e *Exec) {
		e.dir = dir
	}
}

// WithDryrun configures Exec to run in dryrun mode.
func WithDryrun() func(e *Exec) {
	return func(e *Exec) {
		e.knobs.dryrun = true
	}
}

// WithIntercept configures Exec to intercept the given command and return the
// given output instead.
func WithIntercept(command, output string) func(e *Exec) {
	return func(e *Exec) {
		if e.knobs.intercept == nil {
			e.knobs.intercept = make(map[string]string)
		}
		e.knobs.intercept[command] = output
	}
}

// LookPath wraps around exec.LookPath, which searches for an executable named
// file in the directories named by the PATH environment variable.
func (e *Exec) LookPath(file string) (string, error) {
	command := fmt.Sprintf("which %s", file)
	e.logger.Print(command)
	return e.Next(command, func(_, _ io.Writer) (string, error) {
		return exec.LookPath(file)
	})
}

// CommandContextSilent is like CommandContext, but does not take over
// stdout/stderr. It's used for "internal" operations, and always blocks.
func (e *Exec) CommandContextSilent(
	ctx context.Context, name string, args ...string,
) ([]byte, error) {
	return e.commandContextImpl(ctx, nil, true, name, args...)
}

// CommandContextWithInput is like CommandContext, but stdin is piped from an
// in-memory string, and always blocks.
func (e *Exec) CommandContextWithInput(
	ctx context.Context, stdin, name string, args ...string,
) ([]byte, error) {
	r := strings.NewReader(stdin)
	return e.commandContextImpl(ctx, r, false, name, args...)
}

// CommandContextWithEnv is like CommandContextInheritingStdStreams, but
// accepting an additional argument for environment variables.
func (e *Exec) CommandContextWithEnv(
	ctx context.Context, env []string, name string, args ...string,
) error {
	return e.commandContextInheritingStdStreamsImpl(ctx, env, name, args...)
}

// CommandContextInheritingStdStreams is like CommandContext, but stdin,
// stdout, and stderr are passed directly to the terminal.
func (e *Exec) CommandContextInheritingStdStreams(
	ctx context.Context, name string, args ...string,
) error {
	return e.commandContextInheritingStdStreamsImpl(ctx, nil, name, args...)
}

func (e *Exec) commandContextInheritingStdStreamsImpl(
	ctx context.Context, env []string, name string, args ...string,
) error {
	var command string
	if len(args) > 0 {
		command = fmt.Sprintf("%s %s", name, shellescape.QuoteCommand(args))
	} else {
		command = name
	}
	e.logger.Print(command)

	_, err := e.Next(command, func(_, _ io.Writer) (string, error) {
		cmd := exec.CommandContext(ctx, name, args...)
		// NB: In this function we specifically want to inherit the
		// standard IO streams, so we are not going to capture the
		// `outTrace` or `errTrace`.
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = e.dir
		cmd.Env = env

		if err := cmd.Start(); err != nil {
			return "", err
		}
		if e.blocking {
			if err := cmd.Wait(); err != nil {
				return "", err
			}
		}
		return "", nil
	})

	return err
}

func (e *Exec) commandContextImpl(
	ctx context.Context, stdin io.Reader, silent bool, name string, args ...string,
) ([]byte, error) {
	var command string
	if len(args) > 0 {
		command = fmt.Sprintf("%s %s", name, shellescape.QuoteCommand(args))
	} else {
		command = name
	}
	e.logger.Print(command)

	output, err := e.Next(command, func(outTrace, errTrace io.Writer) (string, error) {
		cmd := exec.CommandContext(ctx, name, args...)
		var buffer bytes.Buffer
		if silent {
			cmd.Stdout = io.MultiWriter(&buffer, outTrace)
			cmd.Stderr = errTrace
		} else {
			cmd.Stdout = io.MultiWriter(e.stdout, &buffer, outTrace)
			cmd.Stderr = io.MultiWriter(e.stderr, errTrace)
		}
		if stdin != nil {
			cmd.Stdin = stdin
		}
		cmd.Dir = e.dir

		if err := cmd.Start(); err != nil {
			return "", err
		}
		if err := cmd.Wait(); err != nil {
			return "", err
		}
		return buffer.String(), nil
	})

	if err != nil {
		return nil, err
	}
	return []byte(output), nil
}

// Next is a thin interceptor for all exec activity, running them through
// testing knobs first.
func (e *Exec) Next(
	command string, f func(outTrace, errTrace io.Writer) (output string, err error),
) (string, error) {
	if e.knobs.intercept != nil {
		if output, ok := e.knobs.intercept[command]; ok {
			return output, nil
		}
	}
	if e.knobs.dryrun {
		return "", nil
	}
	return e.Recorder.Next(command, func() (output string, err error) {
		var outTrace, errTrace bytes.Buffer
		defer func() {
			p := e.logger.Prefix()
			defer e.logger.SetPrefix(p)
			sc := bufio.NewScanner(&outTrace)
			e.logger.SetPrefix(p + "EXEC OUT: ")
			for sc.Scan() {
				e.logger.Println(sc.Text())
			}
			sc = bufio.NewScanner(&errTrace)
			e.logger.SetPrefix(p + "EXEC ERR: ")
			for sc.Scan() {
				e.logger.Println(sc.Text())
			}
		}()
		return f(&outTrace, &errTrace)
	})
}

// IsDryrun returns whether or not this exec is running in "dryrun" mode, which is useful to avoid
// behavior that would otherwise permanently block execution during testing.
func (e *Exec) IsDryrun() bool {
	return e.knobs.dryrun
}
